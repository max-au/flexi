%%-------------------------------------------------------------------
%% @author Maxim Fedorov
%% @doc
%%  flexi: flexible application controller (drop-in replacement for
%%      OTP application_controller).
%%
%%  This code is intended to be open-sourced, most likely contributed
%%      to OTP. Therefore it cannot have any dependencies on internal
%%      components.
%% @end
-module(flexi).
-author("maximfca@gmail.com").

%% Convenience API
-export([
    ensure_all_started/1,
    ensure_all_started/2,
    ensure_all_started/3,
    ensure_all_stopped/1,
    critical_path/1,
    make_release/5,
    deploy/3,
    boot/1,
    boot/2,
    load_dag/3
]).

-include_lib("kernel/include/logger.hrl").

%% @doc
%% Starts an application with all dependencies, including distributed
%%  dependencies. For the time being, it starts all applications in
%%  a single VM.
-spec ensure_all_started(Application) -> {ok, Started} | {error, Reason} when
    Application :: atom(),
    Started :: [atom()],
    Reason :: term().
ensure_all_started(Application) ->
    ensure_all_started(Application, temporary).

-spec ensure_all_started(Application, Type) -> {ok, Started} | {error, Reason} when
    Application :: atom(),
    Type :: 'permanent' | 'transient' | 'temporary',
    Started :: [atom()],
    Reason :: {cycle, [atom()]} | {string(), file:filename()}.
ensure_all_started(Application, Type) ->
    try
        {ToLoad, _} = DAG = load_dag([Application], all, code:get_path()),
        load_apps(ToLoad),
        {Order, Done} = start_concurrent(DAG, Type),
        %% find applications we started
        Started = lists:reverse([O || O <- Order, element(1, maps:get(O, Done)) =:= ok]),
        %% check whether any error occurred while starting, and if yes,
        %%  shut down started apps
        case check_success(maps:to_list(Done)) of
            ok ->
                {ok, Started};
            Error ->
                stop_concurrent(DAG, Started),
                Error
        end
    catch
        throw:{Reason, Extra} ->
            {error, {Reason, Extra}}
    end.

%% @doc Starts Application and all not yet started dependencies concurrently.
%%  Returns statistics gathered while starting.
%% For each application:
%%  1. Timestamp, when application was scheduled to start
%%  2. Timestamp, when application completed startup
%%  3. Dependencies list
-type start_result() :: #{
    result := ok | {error, term()}, %% result of application:start() call
    order := non_neg_integer(),     %% index in start sequence
    start := integer(),             %% monotonic_time() when start() was called
    started := integer(),           %% when start() returned
    local := [atom()],              %% list of local dependencies
    distributed => [atom()],        %% distributed dependencies
    longest => {non_neg_integer(), [atom()]}    %% longest path
}.

-type start_map() :: #{atom() => start_result()}.

%% Additional options passed to ensure_all_started.
-type options() :: #{
    %% when specified (and not 'all'), list of distributed
    %%  dependencies that are allowed to be started
    allowed_distributed => all | [atom()],
    %% how to get code paths, via code or erl_prim_loader
    loader => code | prim
}.

-spec ensure_all_started(Apps, Type, Options) -> {ok, Started} | {error, Reason} when
    Apps :: atom() | [atom()],
    Type :: 'permanent' | 'transient' | 'temporary',
    Options :: options(),
    Started :: start_map(),
    Reason :: {cycle, [atom()]} | {string(), file:filename()}.
ensure_all_started(Apps, Type, Options) when is_atom(Apps); is_list(Apps) ->
    try
        ToStart = if is_atom(Apps) -> [Apps]; true -> Apps end,
        {DAG, Roots} = load_dag(ToStart,
            maps:get(allowed_distributed, Options, all),
            get_paths(maps:get(loader, Options, code))
        ),
        load_apps(DAG),
        {Order, Done} = start_concurrent({DAG, Roots}, Type),
        case check_success(maps:to_list(Done)) of
            ok ->
                %% merge stuff from Done into DAG
                {Started, _MaxIndex} = lists:foldl(
                    fun (App, {Acc, Ord}) ->
                        {Result, StartT, StartedT} = maps:get(App, Done),
                        {Strong, Weak, _Spec} = maps:get(App, DAG),
                        Acc1 = Acc#{
                            App =>
                            #{
                                result => Result, order => Ord, start => StartT,
                                started => StartedT, local => Strong, distributed => Weak
                            }
                        },
                        {Acc1, Ord + 1}
                    end, {#{}, 1}, Order),
                {ok, Started};
            Error ->
                stop_concurrent({DAG, Roots}, [O || O <- Order, element(1, maps:get(O, Done)) =:= ok]),
                Error
        end
    catch
        throw:{Reason, Extra} ->
            {error, {Reason, Extra}}
    end.

%% @doc Calculates critical start path, provided with extended
%%  output of ensure_all_started(), and puts it back in the map.
%% Returns critical path of app() + time
-spec critical_path(start_map()) -> {TotalTime :: non_neg_integer(),
    Path :: [{App :: atom(), AppStartTime :: non_neg_integer()}], start_map()}.
critical_path(DAG) ->
    %% trick: add a vertex that depends on all vertices in the graph
    Edges = maps:keys(DAG),
    Root = #{local => Edges, start => 0, started => 0},
    #{'$root' := Crit} = DAG1 = find_path('$root', Edges, DAG#{'$root' => Root}),
    #{longest := {Time, ['$root' | Path]}} = Crit,
    PathWithTimes = [{V, start_time(maps:get(V, DAG1))} || V <- Path],
    {Time, PathWithTimes, maps:remove('$root', DAG1)}.

%% @doc
%% Stops applications previously started. In addition to upstream OTP,
%%  this function logs shutdowns stats, - time taken, etc.
%% Shutdown also happens concurrently, respecting dependencies between
%%  applications.
-spec ensure_all_stopped([atom()] | start_map()) -> ok | {error, term()}.
ensure_all_stopped(DAG) when is_map(DAG) ->
    %% Convert from extended start map results into simple form
    %%  suitable for shutdown
    {Started, SimpleDAG} = maps:fold(
        fun (App, #{local := L, result := Res} = SR, {S, SmDAG}) ->
            SmDAG1 = SmDAG#{App => {L, maps:get(distributed, SR, []), ""}},
            if Res =:= ok -> {[App | S], SmDAG1};
                true -> {S, SmDAG} end
        end, {[], #{}}, DAG),
    stop_concurrent({SimpleDAG, []}, Started);
ensure_all_stopped(Stop) when is_list(Stop) ->
    try
        stop_concurrent(load_dag(Stop, all, code:get_path()), Stop),
        ok
    catch
        throw:{Reason, Extra} ->
            {error, {Reason, Extra}}
    end.

%% Sys Config type
-type sys_config() :: [{App :: atom(), [{Term :: atom(), Value :: term()}]}].

-type vm_args() :: [string()].

%% Release configuration options. File names to read from relx/rebar config,
%%  or their alternatives directly embedded.
-type relx_options() :: [
    {sys_config_src, file:filename() | sys_config()} |
    {vm_args_src, file:filename() | vm_args()}
].

%% @doc
%%  systools boot script adapter for flexi.
%%
%% `make' enumerates dependencies of all requested applications
%%  to add. All local transitive dependencies are added
%%  to the release. Distributed applications dependencies are
%%  not included, unless explicitly specified.
%%
%% Included applications are added to the release with type "load".
%% Dependencies for included_applications are resolved as for normal
%%  applications (which is systools behaviour).
%%
%% OTP systools module used to create standard boot script. OTP
%%  application boot instructions are replaced with
%%  flexi for startup concurrency.
%% sasl is explicitly added as permanent application.
%%
%% Resulting release is created with local filesystem paths,
%%  avoiding the need to create symlinks (like relx does for
%%  dev releases).
%% Resulting release does not contain sys.config or vm.args.
%% These files should be created as a part of deployment step,
%%  which is highly unique for a particular environment. In general,
%%  deployment step should create a target system as required by
%%  OTP principles.
%% An example of this deployment step is provided as `deploy'
%%  function below.
-spec make_release(TargetDir :: file:filename(), RelName :: string(), RelVsn :: string(),
    Apps :: [atom()], Options :: relx_options()) ->
    {ok, BootFile :: file:filename()} | {error, term()}.
make_release(TargetDir, RelName, RelVsn, Apps, Options)
    when is_list(RelName), is_list(RelVsn), is_list(Apps), is_list(Options) ->
    create_release(TargetDir, RelName, RelVsn, Options, load_dag(Apps, Apps, code:get_path())).

%% @doc Creates a target system based on boot script provided.
%%      Writes sys.config and vm.args files, replacing OS environment
%%      variables found in these files (similar to `RELX_REPLACE_OS_VARS=true'
%%      in the extended startup script).
-spec deploy(Target :: file:filename(), Boot :: file:filename(), Env :: #{string() => string()}) -> ok.
deploy(Target, Boot, Env) ->
    {ok, Bin} = file:read_file(Boot ++ ".boot"),
    {script, {_RelStr, RelVsn}, _Script} = binary_to_term(Bin),
    RelDir = filename:join([Target, "releases", RelVsn]),
    SysConfig = filename:join(RelDir, "sys.config"),
    VmArgs = filename:join(RelDir, "vm.args"),
    ok = filelib:ensure_dir(SysConfig),

    %% It would be good to have release_handler:create_RELEASES call here as well, so
    %%  release looks like a normal one. But for now, let's only write start_erl.data
    Vsn = lists:flatten(io_lib:format("~s ~s", [erlang:system_info(version), RelVsn])),
    ok = file:write_file(filename:join([Target, "releases", "start_erl.data"]), Vsn),
    %% materialise configs
    OrigDir = filename:dirname(Boot),
    ok = replace_vars(filename:join(OrigDir, "sys.config"), SysConfig, Env),
    ok = replace_vars(filename:join(OrigDir, "vm.args"), VmArgs, Env),
    %% It is also possible to write 'bin/start' as OTP expects, according
    %%  to documentation:
    %% http://erlang.org/doc/design_principles/applications.html#directory-structure-for-a-released-system
    %% However this script is unlikely to be used often, and it
    %%  can simply be replaced with doc telling to run:
    %%   erl -boot releases/Vsn/RelName.boot -args_file releases/Vsn/vm.args -config releases/Vsn/sys.config
    ok = file:write_file(filename:join([RelDir, "start.boot"]), Bin).

%% @doc Boot the target system extracted from BootFile in
%%      current node. Returns DAG to stop with flexi:ensure_all_stopped().
%%      Completely ignores vm.args or sys.config settings for the release,
%%      for, first, emulator options cannot be changed in runtime, and, second,
%%      replacing configuration may break already running applications.
-spec boot(file:filename()) -> {ok, Started} | {error, Reason} when
    Started :: start_map(),
    Reason :: {cycle, [atom()]} | {string(), file:filename()}.
boot(BootFile) ->
    {ok, Bin} = file:read_file(BootFile ++ ".boot"),
    {script, {_RelStr, _RelVsn}, Script} = binary_to_term(Bin),
    [{Roots, Opts}] = [{R, Opts} || {apply, {flexi, ensure_all_started, [R, permanent, Opts]}} <- Script],
    flexi:ensure_all_started(Roots, temporary, Opts#{loader => code}).

%% @doc Boots Target System in a new Erlang node. Expected workflow is
%%      to make the release via `make_release', then `deploy' it somewhere,
%%      and then `boot' a new peer running this release.
%%      Peer node is linked to a calling process.
%%      Looks for sys.config and vm.args in the TargetSystem/releases/...
%%      and adds it to command line.
-spec boot(file:filename(), peer:start_options()) -> {ok, pid()} | {ok, pid(), node()} | {error, term()}.
boot(TargetSystem, Options) ->
    %% read start_erl.data to find the appropriate release
    {ok, Bin} = file:read_file(filename:join([TargetSystem, "releases", "start_erl.data"])),
    [_Erts, RelVsn] = binary:split(Bin, <<" ">>, [global]),
    RelRoot = filename:join([TargetSystem, "releases", binary_to_list(RelVsn)]),
    %% add sys.config and vm.args to command line args, if files are there
    %% NB: this will be merged with -args or -config if user already passed.
    %% merge rule: for "-config C1 -confg C2", C2 overrides C1 for overlapped parameters;
    %% otherwise, add parameters.
    UserArgs = maps:get(args, Options, []),
    Args1 = maybe_add("-config", filename:join([RelRoot, "sys.config"]), UserArgs),
    Args2 = maybe_add("-args_file", filename:join([RelRoot, "vm.args"]), Args1),
    BootScript = filename:join([RelRoot, "start"]),
    Args3 = Args2 ++ ["-boot", BootScript],
    peer:start_link(Options#{args => Args3}).

%% -------------------------------------------------------------------
%% Internal implementation

%% Scan all module binary paths and load all of them.
%%
%% We do a very special optimization here because we might have lots of applications.
%%
%% In OTP, when we load a module, we are given a module name,
%% and we scan through potential paths one-by-one to find the
%% first corresponding .beam file.
%%
%% In flexi, the optimization is: we first list all the beam files in the paths,
%% and then add them one by one (if not already loaded), this avoids
%% (# of modules) * (# of paths) operations.
%%
%% An additional optimization to speed this up is to use code:atomic_load, it is fast
%% when loading multiple modules at the same time, but it can also easily fails because
%% various reasons. We do 100 modules at a time at this moment.
-spec load_modules() -> ok.
load_modules() ->
    SystemApps = [
        asn1,
        compiler,
        crypto,
        inets,
        kernel,
        os_mon,
        public_key,
        runtime_tools,
        sasl,
        ssl,
        stdlib,
        tools,
        xmerl
    ],
    Applications = [App || {App, _, _} <- application:loaded_applications()],
    AppModules = [
        application:get_key(App, modules)
    || App <- Applications -- SystemApps],
    Modules = maps:from_list([
        {Module, true}
    ||
        Module <- lists:flatten([Mods || {ok, Mods} <- AppModules])
    ]),
    Ext = init:objfile_extension(),
    {_ModuleMap, ModuleList} = lists:foldl(
        fun(Path, Acc) ->
            lists:foldl(
                fun(File, {AccMap, AccList}) ->
                    Module = list_to_atom(filename:basename(File, Ext)),
                    case
                        is_map_key(Module, Modules)
                        andalso not is_map_key(Module, AccMap)
                        andalso code:is_loaded(Module) =:= false
                    of
                        true ->
                            {ok, Bin} = file:read_file(File),
                            {AccMap#{Module => File}, [{Module, File, Bin} | AccList]};
                        false ->
                            {AccMap, AccList}
                    end
                end,
                Acc,
                filelib:wildcard(filename:join(Path, "*" ++ Ext))
            )
        end,
        {#{}, []},
        code:get_path()
    ),
    do_load_modules(ModuleList, [], 100).

do_load_modules([], [], _) ->
    ok;
do_load_modules(All, Acc, N) when All =:= []; N =:= 0 ->
    case code:atomic_load(Acc) of
        ok ->
            do_load_modules(All, [], 100);
        {error, Failures} ->
            FailedModules = maps:from_list(Failures),
            % wacov adds on_load attribute to almost every file, so this
            % do_load_modules function would load nothing, here we revert
            % back to loading one by one for this case
            [
                code:load_binary(Module, File, Bin)
             || {Module, File, Bin} <- Acc,
                maps:get(Module, FailedModules, notfound) =:= on_load_not_allowed
            ],
            Retry = [Item || {Module, _, _} = Item <- Acc, not is_map_key(Module, FailedModules)],
            do_load_modules(All, Retry, N)
    end;
do_load_modules([Head | Rest], Acc, N) ->
    do_load_modules(Rest, [Head | Acc], N - 1).

get_paths(code) ->
    code:get_path();
get_paths(prim) ->
    {ok, Paths} = erl_prim_loader:get_path(),
    Paths.

%% Quickly loads DAG into application_controller
load_apps(DAG) ->
    {Loaded, _, _} = lists:unzip3(application:loaded_applications()),
    [case application:load({application, App, Spec}) of
         ok -> ok;
         {error, {already_loaded, _}} -> ok
        %% intentionally crash for any real/unknown error
     end
    || {App, {_, _, Spec}} <- maps:to_list(DAG), not lists:member(App, Loaded)],
    load_modules().

%%  DAG in form of map, where key is the application name,
%%   and value is a tuple of {Local, Distributed, AppSpec}. Local means
%%   applications running locally, Distributed means applications
%%   that are distributed dependencies (not necessarily local).
%%  AppSpec is useful for faster/cached access needed to
%%   quickly load hundreds of applications into application_controller.
-type spec_dag() :: #{
    atom() => {Local :: [atom()], Distributed :: [atom()], Spec :: proplists:proplist()}
}.

%% @doc Loads DAG of application specifications.
%%      Accepts a list of applications to be included in the release,
%%      and a list of allowed distributed applications to be included
%%      in the release. Current OTP version requires that a distributed
%%      application code is added to all releases using this application,
%%      however we do not want to ship that much dead code. If you want
%%      to follow all distributed dependencies, use atom 'all'.
%%      Returns a DAG, and a list of all distributed applications
%%      that are allowed (and expected) to start locally.
%%
%%      CodePaths are code search paths, to look for application spec files.
%%      Could be taken from dynamic code paths (code:get_path()), or boot
%%      script paths from erl_prim_load (populated in init script).
-spec load_dag(
    Apps :: [atom()],
    AllowedDist :: [atom()] | all,
    CodePaths :: [Dir :: file:filename()]) -> {spec_dag(), RootApps :: [atom()]}.
load_dag(Roots, AllowedDist, CodePaths) ->
    load_dag(Roots, AllowedDist, Roots, #{}, #{}, CodePaths).

load_dag([], _AllowedDist, Roots, DAG, _AppCache, _Path) ->
    %% find any cycles in strong dependencies
    [ensure_no_cycle(DAG, Root, #{}) || Root <- Roots],
    {DAG, Roots};

load_dag([App | Apps], AllowedDist, Roots, DAG, AppCache, Path) when is_map_key(App, DAG) ->
    load_dag(Apps, AllowedDist, Roots, DAG, AppCache, Path);
load_dag([App | Apps], AllowedDist, Roots, DAG, AppCache, Path) ->
    FName = atom_to_list(App) ++ ".app",
    %% code:where_is_file does not use cached results of filesystem queries
    case find_and_consult(AppCache, FName, Path) of
        {{ok, [{application, App, Spec}]}, AppCache1, Path1} ->
            %% included applications don't need to be started, but
            %%  are part of a release
            Included = proplists:get_value(included_applications, Spec, []),
            %% dependencies
            Deps = proplists:get_value(applications, Spec, []),
            %% distributed dependencies added to roots
            Weak = filter_allowed(
                proplists:get_value(distributed_dependencies, Spec, []),
                AllowedDist),
            load_dag(Apps ++ Deps ++ Weak ++ Included, AllowedDist, lists:usort(Weak ++ Roots),
                DAG#{App => {Deps, Weak, Spec}}, AppCache1, Path1);
        {{error, Reason}, _, _} ->
            throw({file:format_error(Reason), FName})
    end.

%% filters distributed dependencies, leaving only those allowed, unless 'all' are allowed
filter_allowed(Deps, all) ->
    Deps;
filter_allowed(Deps, Allowed) ->
    [Dep || Dep <- Deps, lists:member(Dep, Allowed)].

%% code:where_is_file is slow, and does not cache file system
find_and_consult(AppCache, File, Path) ->
    case maps:take(File, AppCache) of
        {AbsPath, AppCache1} ->
            {file:consult(AbsPath), AppCache1, Path};
        error when Path =:= [] ->
            {{error, enoent}, AppCache, Path};
        error ->
            [Next | Path1] = Path,
            case erl_prim_loader:list_dir(Next) of
                {ok, Files} ->
                    %% strip ".app" extension
                    AppCache1 = lists:foldl(
                        fun (F, Acc) ->
                            case filename:extension(F) of
                                ".app" ->
                                    Acc#{F => filename:join(Next, F)};
                                _Ext ->
                                    Acc
                            end
                        end, AppCache, Files),
                    find_and_consult(AppCache1, File, Path1);
                _Error ->
                    find_and_consult(AppCache, File, Path1)
            end
    end.

%% DFS to find a cycle in the graph (and throw)
%% Visited: contains "true" for already checked vertices,
%%  or a number - index in the stack.
ensure_no_cycle(DAG, Vertex, Visited) ->
    Visited1 = Visited#{Vertex => map_size(Visited)},
    {Strong, _Weak, _Spec} = maps:get(Vertex, DAG),
    Visited2 = lists:foldl(
        fun (Next, Vis) ->
            case maps:find(Next, Vis) of
                {ok, true} ->
                    Vis;
                {ok, _Index} ->
                    {Stack, _} = lists:unzip(lists:keysort(2,
                        [{V, N} || {V, N} <- maps:to_list(Visited), is_integer(N)])),
                    Cycle = Stack ++ [Vertex, Next],
                    throw({cycle, Cycle});
                error ->
                    ensure_no_cycle(DAG, Next, Vis)
            end
        end, Visited1, Strong),
    Visited2#{Vertex => true}.

%% Starts applications, as concurrently as possible
start_concurrent({DAG, _Roots}, Type) ->
    %% filter out already started apps
    {running, Apps} = lists:keyfind(running, 1, application_controller:info()),
    {Running, _} = lists:unzip(Apps),
    StartOnly = maps:without(Running, DAG),
    Done = maps:from_list([{App, {{error, {already_started, App}}, 0, 0}} || App <- Running]),
    do_concurrent(StartOnly, Type, #{}, [], Done).

stop_concurrent({DAG, _Roots}, Include) ->
    %% reverse the DAG
    Reversed = maps:fold(
        fun (App, {Strong, _Weak, _Spec}, Acc) ->
            lists:foldl(
                fun (Dep, Inner) ->
                    maps:update_with(Dep,
                        fun (Existing) ->
                            lists:umerge(Existing, [App])
                        end, [App], Inner)
                end, Acc, Strong)
        end, maps:from_list([{A, []} || A <- maps:keys(DAG)]), DAG),
    %% remove all dependencies for non-included applications
    StopOnly = maps:fold(
        fun (App, Strong, Acc) ->
            case lists:member(App, Include) of
                false ->
                    Acc;
                true ->
                    FilteredDeps = lists:filter(
                        fun (Dep) ->
                            lists:member(Dep, Include)
                        end, Strong),
                    Acc#{App => {FilteredDeps, [], ""}}
            end
        end, #{}, Reversed),
    do_concurrent(StopOnly, stop, #{}, [], #{}).

%% Executes a graph as concurrently as possible
do_concurrent(Apps, _Type, InProgress, Order, Done) when map_size(Apps) =:= 0, map_size(InProgress) =:= 0 ->
    {Order, Done};
do_concurrent(Apps, Type, InProgress, Order, Done) when map_size(Apps) =:= 0 ->
    wait(Apps, Type, InProgress, Order, Done);
do_concurrent(Apps, Type, InProgress, Order, Done) ->
    case find_ready(maps:iterator(Apps), Done) of
        false ->
            wait(Apps, Type, InProgress, Order, Done);
        {App, ok} ->
            %% all dependencies started successfully
            Control = self(),
            _Pid = spawn_link(
                fun () ->
                    Control ! {done, App, timed(App, Type)}
                end),
            do_concurrent(maps:remove(App, Apps), Type,
                InProgress#{App => erlang:monotonic_time()}, [App | Order], Done);
        {App, Result} ->
            %% dependency failed to start
            do_concurrent(maps:remove(App, Apps), Type,
                InProgress, [App | Order], Done#{App => {Result, 0, 0}})
    end.

wait(Apps, Type, InProgress, Order, Done) ->
    receive
        {done, App, Result} ->
            {Started, Starting1} = maps:take(App, InProgress),
            do_concurrent(Apps, Type, Starting1, Order,
                Done#{App => {Result, Started, erlang:monotonic_time()}})
    end.

%% find any application that has all dependencies
%%  satisfied (not necessarily successfully)
find_ready(Iter, Done) ->
    case maps:next(Iter) of
        none ->
            false;
        {App, {Strong, _Weak, _Spec}, NextIter} ->
            case satisfied(Strong, Done, ok) of
                false ->
                    find_ready(NextIter, Done);
                Result ->
                    {App, Result}
            end
    end.

satisfied([], _Done, Result) ->
    Result;
satisfied([Dep | Deps], Done, Result) ->
    case maps:find(Dep, Done) of
        error ->
            false;
        {ok, {ok, _, _}} ->
            satisfied(Deps, Done, Result);
        {ok, {{error, {already_started, Dep}}, _, _}} ->
            satisfied(Deps, Done, Result);
        {ok, {Error, _, _}} ->
            Error
    end.

check_success([]) ->
    ok;
check_success([{_App, {ok, _, _}} | Apps]) ->
    check_success(Apps);
check_success([{App, {{error, {already_started, App}}, _, _}} | Apps]) ->
    check_success(Apps);
check_success([{_App, {Error, _, _}} | _Apps]) ->
    Error.

%% -------------------------------------------------------------------
%% Internal implementation: start/stop with extended logging, as compared
%%  to simple SASL logging.

timed(App, stop) ->
    StartTime = erlang:monotonic_time(),
    Result = hacky_stop(App),
    FinishTime = erlang:monotonic_time(),
    Time = erlang:convert_time_unit(FinishTime - StartTime, native, millisecond),
    ?LOG_NOTICE(
        #{
            application => App,
            exited => stop,
            shutdown => Time
        },
        #{domain => [flexi], report_cb => fun format_log/2}),
    Result;

timed(App, RestartType) ->
    StartTime = erlang:monotonic_time(),
    Result = application:start(App, RestartType),
    FinishTime = erlang:monotonic_time(),
    StartTook = erlang:convert_time_unit(FinishTime - StartTime, native, millisecond),
    ?LOG_NOTICE(
        #{
            application => App,
            start => StartTook,
            result => Result
        },
        #{domain => [flexi], report_cb => fun format_log/2}),
    Result.

%% -------------------------------------------------------------------
%% Another DFS to find longest path
find_path(Vertex, [], DAG) ->
    #{local := Edges} = StartRes = maps:get(Vertex, DAG),
    %% all Edges of Vertex are known
    {Longest, Path} = lists:foldl(
        fun (E, {Crit, CP}) ->
            case maps:get(E, DAG, undefined) of
                #{longest := {MaybeL, MaybePath}} when MaybeL > Crit ->
                    {MaybeL, MaybePath};
                _ ->
                    {Crit, CP}
            end
        end, {0, []}, Edges),
    DAG#{Vertex => StartRes#{longest => {Longest + start_time(StartRes), [Vertex | Path]}}};
find_path(Vertex, [E | Es], DAG) ->
    case maps:get(E, DAG, undefined) of
        StartRes when is_map_key(longest, StartRes) ->
            find_path(Vertex, Es, DAG);
        #{local := Local} ->
            find_path(Vertex, Es, find_path(E, Local, DAG));
        undefined ->
            find_path(Vertex, Es, DAG)
    end.

start_time(#{start := StartT, started := StartedT}) ->
    erlang:convert_time_unit(StartedT - StartT, native, millisecond).

%% -------------------------------------------------------------------
%% Below lies the horrible hack to make application shutdown
%%  concurrent.

-record(state, {loading, starting, start_p_false, running,
    control, started, start_req, conf_data}).

%% init_stop is executed in the context of application controller,
%%  it can perform all operations (for distributed applications),
%%  or send a message back to caller.
init_stop(AppName, Caller, #state{running = Running, started = Started} = State) ->
    case lists:keyfind(AppName, 1, Running) of
        {_AppName, AppMaster} ->
            {_AppName2, Type} = lists:keyfind(AppName, 1, Started),
            Caller ! {stop, AppMaster, Type},
            case AppMaster of
                Pid when is_pid(Pid) ->
                    unlink(AppMaster),
                    State;
                undefined ->
                    %% Code-only application stopped
                    info_exited(AppName, stopped, Type),
                    done_stop(AppName, State);
                _ ->
                    %% Distributed application stopped
                    done_stop(AppName, State)
            end;
        false ->
            case lists:keymember(AppName, 1, Started) of
                true ->
                    NStarted = lists:keydelete(AppName, 1, Started),
                    cntrl(AppName, State, {ac_application_stopped, AppName}),
                    State#state{started = NStarted};
                false ->
                    State
            end
    end.

%% done_stop is only called when local application is done
%%  stopping.
done_stop(AppName, #state{running = Running, started = Started} = State) ->
    NRunning = lists:keydelete(AppName, 1, Running),
    NStarted = lists:keydelete(AppName, 1, Started),
    cntrl(AppName, State, {ac_application_stopped, AppName}),
    State#state{running = NRunning, started = NStarted}.

hacky_stop(AppName) ->
    Control = self(),
    %% first stage: attempt to stop
    sys:replace_state(application_controller, fun (State) -> init_stop(AppName, Control, State) end),
    %% wait for response from hacked controller
    receive
        {stop, AppMaster, Type} when is_pid(AppMaster) ->
            %% wait for actual stop here, outside of application controller.
            %% This presents an opportunity for some race condition, which we
            %%  don't handle yet.
            Tag = make_ref(),
            Ref = erlang:monitor(process, AppMaster),
            AppMaster ! {stop, Tag, self()},
            receive
                {'DOWN', Ref, process, _, _Info} ->
                    ok;
                {Tag, Res} ->
                    erlang:demonitor(Ref, [flush]),
                    Res
            end,
            %% proceed with removal from application_controller
            sys:replace_state(application_controller,
                fun (State) ->
                    info_exited(AppName, stopped, Type),
                    ets:delete(ac_tab, {application_master, AppName}),
                    done_stop(AppName, State)
                end),
            ok;
        {stop, _, _} ->
            %% everything was already done
            ok
    end.

cntrl(AppName, #state{control = Control}, Msg) ->
    case lists:keyfind(AppName, 1, Control) of
        {_AppName, Pid} ->
            Pid ! Msg,
            true;
        false ->
            false
    end.

info_exited(Name, Reason, Type) ->
    ?LOG_NOTICE(#{label=>{application_controller,exit},
        report=>[{application, Name},
            {exited, Reason},
            {type, Type}]},
        #{domain=>[otp],
            report_cb=>fun logger:format_otp_report/1,
            error_logger=>#{tag=>info_report,type=>std_info}}).

%% ^^^ horrible hack ends
%% -------------------------------------------------------------------

%% Formatter for custom logging
format_log(#{application := App, exited := Reason, shutdown := Time}, _Config) ->
    io_lib:format("~s stopped in ~b ms, reason: ~p~n", [App, Time, Reason]);
format_log(#{application := App, start := ST, result := Result}, _Config) ->
    io_lib:format("~s started in ~b ms, result: ~p~n", [App, ST, Result]).

%%--------------------------------------------------------------------
%% Release implementation

%% Creates release from Local applications (included in the release)
%%  and Remote (excluded, replaced with stubs for systools call).
%% This function mostly repeats what relx is doing, but omitting several
%%  steps for performance reasons, and adding some for concurrent startup.
create_release(TargetDir, RelName, RelVsn, Options, {DAG, Roots}) ->
    BaseFile = filename:join(TargetDir, RelName),
    ok = filelib:ensure_dir(BaseFile),
    %% write release spec, *.rel file
    Local0 = maps:fold(
        fun (App, {_, _, Spec}, Acc) ->
            [{App, proplists:get_value(vsn, Spec)} | Acc]
        end, [], DAG),
    %% Add SASL & flexi applications. SASL needed for releases, flexi - for concurrent start
    _ = application:load(flexi), {ok, FlexiVsn} = application:get_key(flexi, vsn),
    _ = application:load(sasl), {ok, SaslVsn} = application:get_key(sasl, vsn),

    Local1 = lists:foldl(
        fun({Key, Index, El}, Acc) ->
            lists:keystore(Key, Index, Acc, El)
        end,
        Local0,
        [
            {flexi, 1, {flexi, FlexiVsn, load}},
            {sasl, 1, {sasl, SaslVsn}}
        ]
    ),
    %% Same issue that rebar3/relx in escript form has, lacking cross-erts
    %%  releases. Should be fine for testing, which is the original goal.
    RelSpec = {release, {RelName, RelVsn}, {erts, erlang:system_info(version)}, Local1},
    AppSpec = io_lib:fwrite("~p. ", [RelSpec]),
    ok = file:write_file(BaseFile ++ ".rel", lists:flatten(AppSpec)),
    %% make boot script with local paths
    %% If this release is ever to be used outside of local node testing, then
    %%  systools:make call should not use 'local' qualifier, and here we'd need to add
    %%  lib/ folder containing release applications.
    {ok, systools_make, []} = systools:make_script(BaseFile, [silent, {outdir, TargetDir}, local]),
    %% replace non-system (stdlib/kernel/sasl) start with flexi,
    %%  concurrently starting root apps
    {ok, [{script, {RelStr,RelVsn}, OrigScript}]} = file:consult(BaseFile ++ ".script"),
    UpdatedScript = flexi_startup(OrigScript, Roots, []),
    RelScript = {script, {RelStr,RelVsn}, UpdatedScript},
    %% write down amended boot script
    Script = lists:flatten(io_lib:format("%% ~s\n%% flexi: script generated at ~w ~w\n~tp.\n",
        [epp:encoding_to_string(utf8), date(), time(), RelScript])),
    ok = file:write_file(BaseFile ++ ".script", Script),
    ok = systools:script2boot(BaseFile),
    %% copy or simply write sys_config
    SysConfigPath = filename:join(TargetDir, "sys.config"),
    case proplists:get_value(sys_config_src, Options) of
        undefined ->
            ok;
        Terms when is_tuple(hd(Terms)) ->
            FlatTerms = lists:flatten(io_lib:format("~tp.\n", [Terms])),
            ok = file:write_file(SysConfigPath, FlatTerms);
        Strings when is_list(hd(Strings)) ->
            SysStrings = lists:flatten(lists:join("\n", Strings)),
            ok = file:write_file(SysConfigPath, SysStrings);
        Filename ->
            {ok, _} = file:copy(Filename, SysConfigPath)
    end,
    %% copy or write vm.args
    VmArgsPath = filename:join(TargetDir, "vm.args"),
    case proplists:get_value(vm_args_src, Options) of
        undefined ->
            ok;
        Strings1 when is_list(hd(Strings1)) ->
            VmContent = lists:flatten(lists:join("\n", Strings1)),
            ok = file:write_file(VmArgsPath, VmContent);
        VmF ->
            {ok, _} = file:copy(VmF, VmArgsPath)
    end,
    {ok, BaseFile}.

replace_vars(Source, Target, Env) ->
    case file:read_file(Source) of
        {ok, Bin} ->
            file:write_file(Target, replace_var(Bin, Env));
        {error, enoent} ->
            ok
    end.

replace_var(Bin, Env) ->
    case re:run(Bin, <<"[$]{[^}]*}">>, [{capture, first, binary}]) of
        nomatch ->
            Bin;
        {match, [Var]} ->
            [$$, ${ | StrVar] = binary_to_list(Var),
            Value = maps:get(lists:droplast(StrVar), Env, ""),
            replace_var(binary:replace(Bin, Var, list_to_binary(Value)), Env)
    end.

maybe_add(Arg, File, Args) ->
    case filelib:is_regular(File) of
        true ->
            [Arg, File] ++ Args;
        false ->
            Args
    end.

flexi_startup([], _Roots, Acc) ->
    lists:reverse(Acc);
flexi_startup([{apply, {application, start_boot, [App, _Type]}} | Tail], Roots, Acc)
    when App =/= stdlib, App =/= kernel, App =/= flexi ->
    %% remove all app boot calls except for kernel and stdlib
    flexi_startup(Tail, Roots, Acc);
flexi_startup([{apply, {c, erlangrc, []}} | Tail], Roots, Acc) ->
    %% insert flexi:ensure_all_started just before reporting 'started'
    flexi_startup(Tail, Roots,
        [
            {apply, {flexi, ensure_all_started, [Roots, permanent, #{allowed_distributed => Roots, loader => prim}]}},
            {apply, {application, ensure_all_started, [flexi]}}
            | Acc]);
flexi_startup([Cmd | Tail], Roots, Acc) ->
    flexi_startup(Tail, Roots, [Cmd | Acc]).
