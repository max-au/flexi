%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov
%%% @doc
%%%  Part of "flexi": flexible topology. Tests implementation of
%%%     distributed application dependencies.
%%% @end
-module(flexi_SUITE).
-author("maximfca@gmail.com").

%% Common Test headers
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% Test server callbacks
-export([
    suite/0,
    groups/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    start_local/0, start_local/1,
    load_error/0, load_error/1,
    start_error/0, start_error/1,
    start_error_distributed/0, start_error_distributed/1,
    start_distributed/0, start_distributed/1,
    loop/0, loop/1,
    distributed_loop/0, distributed_loop/1,
    critical_path/0, critical_path/1,
    find_path/0, find_path/1,
    boot_node/0, boot_node/1
]).

%% Flexi domain logger
-export([
    log/2
]).

suite() ->
    [{timetrap, {seconds, 10}}].

cases() ->
    [start_local, load_error, start_error, start_error_distributed,
        start_distributed, loop, distributed_loop, critical_path, boot_node].

groups() ->
    [{flexi, cases()}, {flexi_controller, cases()}].

%% These tests cannot run concurrently, as they use singleton, application_controller.
all() ->
    [find_path, {group, flexi}].

init_per_suite(Config) ->
    [{code_path, code:get_path()}, {info, application:info()} | Config].

end_per_suite(Config) ->
    Config.

init_per_group(GroupName, Config) ->
    lists:keystore(ct_group, 1, Config, {ct_group, GroupName}).

end_per_group(_GroupName, Config) ->
    lists:keydelete(ct_group, 1, Config).

end_per_testcase(_TestCase, Config) ->
    Info = application:info(),
    OldInfo = ?config(info, Config),
    %% stop extra running
    ExtraRunning = extra(running, Info, OldInfo),
    [_ = application:stop(App) || App <- ExtraRunning],
    %% unload extra loaded
    ExtraLoaded = extra(loaded, Info, OldInfo),
    [_ = application:unload(App) || App <- ExtraLoaded],
    ExtraPaths = code:get_path() -- ?config(code_path, Config),
    [_ = code:del_path(Path) || Path <- ExtraPaths].

%% -------------------------------------------------------------------
%% Helper functions

extra(Key, New, Old) ->
    [App || {App, _, _} <- proplists:get_value(Key, New, []) -- proplists:get_value(Key, Old, [])].

get_app_metadata(Name, Vsn, Deps, DistributedDeps, ModArgs) ->
    AtomName = erlang:list_to_atom(Name),
    {application, AtomName,
        [{description, ""},
            {vsn, Vsn},
            {modules, [AtomName, list_to_atom(lists:concat([Name, "_sup"]))]},
            {included_applications, []},
            {registered, []},
            {mod, {AtomName, ModArgs}},
            {applications, [kernel, stdlib] ++ Deps} |
            [{distributed_dependencies, DistributedDeps} || DistributedDeps =/= undefined]
        ]}.

supervisor(Name, Delay) ->
    [
        "-module(" ++ Name ++ "_sup).",
        "-export([start_link/0, init/1]).",
        "-behaviour(supervisor).",
        "start_link() -> supervisor:start_link({local, " ++ Name ++ "_sup}, " ++ Name ++ "_sup, []).",
            "init(_) -> timer:sleep(" ++ integer_to_list(Delay) ++
            "), {ok, {#{strategy => one_for_all, intensity => 5, period => 600}, []}}."
    ].

application(Name, Delay) ->
    [
        "-module(" ++ Name ++ ").",
        "-export([start/2, stop/1]).",
        "-behaviour(application).",
        "start(_StartType, {control, Pid}) -> {ok, Sup} = " ++ Name ++
            "_sup:start_link(), Pid ! {started, " ++ Name ++ "}, {ok, Sup, {control, Pid}};" ++
            "start(_StartType, _StartArgs) -> " ++ Name ++ "_sup:start_link().",
        "stop({control, Pid}) -> timer:sleep(" ++ integer_to_list(Delay) ++
            "), Pid ! {stopped, " ++ Name ++ "}, ok;" ++
            "stop(_State) -> timer:sleep(" ++ integer_to_list(Delay) ++ "), ok."
    ].

compile_code(Lines) ->
    Tokens = [begin {ok, T, _} = erl_scan:string(L), T end || L <- Lines],
    Forms = [begin {ok, F} = erl_parse:parse_form(T), F end || T <- Tokens],
    {ok, _Module, Binary} = compile:forms(Forms),
    Binary.

create_app(Dir, Name, Version, Deps, DistributedDeps, Delay, ModArgs) ->
    AppName = atom_to_list(Name),
    DirName = filename:join([Dir, AppName, "ebin"]),
    AppFileName = filename:join(DirName, lists:concat([AppName, ".app"])),
    ok = filelib:ensure_dir(AppFileName),
    true = code:add_path(DirName),
    AppSpec = io_lib:fwrite("~p. ", [get_app_metadata(AppName, Version, Deps, DistributedDeps, ModArgs)]),
    ok = file:write_file(AppFileName, lists:flatten(AppSpec)),
    %% write an application callback module, and a supervisor
    SupFileName = filename:join(DirName, lists:concat([AppName, "_sup.beam"])),
    AppCbName = filename:join(DirName, lists:concat([AppName, ".beam"])),
    ok = file:write_file(SupFileName, compile_code(supervisor(AppName, Delay))),
    ok = file:write_file(AppCbName, compile_code(application(AppName, Delay))).

random_vsn() ->
    lists:concat([rand:uniform(10), ".", rand:uniform(10), ".", rand:uniform(10)]).

create_app(Dir, Name, Deps, DistributedDeps) ->
    create_app(Dir, Name, random_vsn(), Deps, DistributedDeps, 0, []).

create_app(Dir, Name, Deps, DistributedDeps, Delay, ModArgs) ->
    create_app(Dir, Name, random_vsn(), Deps, DistributedDeps, Delay, ModArgs).

%% logging helpers: use OTP logger sink to extract app start/stop time
intercept_logger() ->
    ok = logger:add_handler(?MODULE, ?MODULE, #{
        filter_default => stop,
        filters => [{domain, {fun logger_filters:domain/2, {log, equal, [flexi]}}}],
        config => #{control => self()}
    }).

log(#{msg := {report, Report}}, #{config := #{control := Control}}) ->
    Control ! {report, Report};
log(_Report, _Config) ->
    ok.

flush_log(Stats) ->
    receive
        {report, Report} when is_map_key(shutdown, Report); is_map_key(start, Report) ->
            flush_log([Report | Stats])
    after 0 ->
        logger:remove_handler(?MODULE),
        Stats
    end.

fetch_msg_queue(Acc) ->
    receive
        Msg ->
            fetch_msg_queue([Msg | Acc])
    after 1 ->
        lists:reverse(Acc)
    end.

%% -------------------------------------------------------------------
%% Test Cases

start_local() ->
    [{doc, "Tests that simple dependency tree starts"}].

start_local(Config) when is_list(Config) ->
    Ctrl = ?config(ct_group, Config),
    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    create_app(Priv, left, [], [], 1000, {control, ?FUNCTION_NAME}),
    create_app(Priv, right, [], [], 1000, {control, ?FUNCTION_NAME}),
    create_app(Priv, center, [left, right], [], 0, {control, ?FUNCTION_NAME}),
    Running = lists:keyfind(running, 1, application_controller:info()),
    Expected = [left, right, center],
    %%
    true = register(?FUNCTION_NAME, self()),
    %% left + right start concurrently, so overall it cannot take more than 2 sec
    StartTime = erlang:monotonic_time(),
    ?assertEqual({ok, Expected}, Ctrl:ensure_all_started(center)),
    %% check the timings
    StartedTime = erlang:monotonic_time(),
    InitTime = erlang:convert_time_unit(StartedTime - StartTime, native, millisecond),
    ?assert(InitTime < 2000, "Startup is not concurrent"),
    ?assertEqual(ok, Ctrl:ensure_all_stopped(Expected)),
    %% check that shutdown is also concurrent
    ShutTime = erlang:convert_time_unit(erlang:monotonic_time() - StartedTime, native, millisecond),
    %% make sure same set of apps as before is running
    Running2 = lists:keyfind(running, 1, application_controller:info()),
    ?assertEqual(Running, Running2),
    %% shutdown should be concurrent (hacky, but still)
    ?assert(ShutTime < 2000, "Shutdown is not concurrent"),
    unregister(?FUNCTION_NAME),
    %% verify sequence (center must be started after, and shut down before left/right)
    ?assertMatch([{started, _}, {started, _}, {started, center},
        {stopped, center}, {stopped, _}, {stopped, _}], fetch_msg_queue([])),
    ok.

load_error() ->
    [{doc, "Tests that error when loading apps does not start anything"}].

load_error(Config) when is_list(Config) ->
    Ctrl = ?config(ct_group, Config),
    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    create_app(Priv, right, [], []),
    create_app(Priv, center, [right, error], []),
    {running, Running} = lists:keyfind(running, 1, application_controller:info()),
    Expected = {error,{"no such file or directory","error.app"}},
    ?assertEqual(Expected, Ctrl:ensure_all_started(center)),
    %% make sure same set of apps as before is running
    {running, Running2} = lists:keyfind(running, 1, application_controller:info()),
    ?assertEqual([], Running2 -- Running, "extra apps left").

start_error() ->
    [{doc, "Tests that error in strong dependencies shuts everything down"}].

start_error(Config) when is_list(Config) ->
    Ctrl = ?config(ct_group, Config),
    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    create_app(Priv, right, [], [], 0, {control, ""}),
    create_app(Priv, center, [right], []),
    {running, Running} = lists:keyfind(running, 1, application_controller:info()),
    ?assertMatch({error, {bad_return, _}}, Ctrl:ensure_all_started(center)),
    %% make sure same set of apps as before is running
    {running, Running2} = lists:keyfind(running, 1, application_controller:info()),
    ?assertEqual([], Running2 -- Running, "extra apps left").

start_error_distributed() ->
    [{doc, "Tests that error in weak dependencies is an error"}].

start_error_distributed(Config) when is_list(Config) ->
    Ctrl = ?config(ct_group, Config),
    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    create_app(Priv, right, [], [], 0, {control, ""}), %% trigger a crash on app startup
    create_app(Priv, center, [], [right]),
    {running, Running} = lists:keyfind(running, 1, application_controller:info()),
    ?assertMatch({error, {bad_return, {{right,start,[normal,{control,[]}]}, _}}},
        Ctrl:ensure_all_started(center)),
    %% make sure same set of apps as before is running
    {running, Running2} = lists:keyfind(running, 1, application_controller:info()),
    ?assertEqual([], Running2 -- Running, "extra apps left").

start_distributed() ->
    [{doc, "Starts an application and distributed dependencies"}].

%% Currently this test case only works for a local applications,
%%  running in the same BEAM. Later on, additional test cases
%%  will be added to support really distributed fashion.
start_distributed(Config) when is_list(Config) ->
    Ctrl = ?config(ct_group, Config),
    intercept_logger(),

    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    %% topology: app_1 -> [app2 (dist_1), app3 (dist_2)].
    create_app(Priv, dist1, [], undefined),
    create_app(Priv, dist2, [], []),
    create_app(Priv, app3, [], [dist2]),
    create_app(Priv, app2, [], [dist1]),
    create_app(Priv, app1, [app2, app3], []),

    %% increasing test coverage: start one of the apps...
    ok = application:start(dist2),
    ok = application:start(app3),

    %% it would be great to add property-based test here
    ExpectedApps = [app1, app2, dist1],
    %% remember set of apps running
    Running = lists:keyfind(running, 1, application_controller:info()),

    %% verify startup order is compatible with application_controller
    ?assertMatch({ok, [_, _, app1]}, Ctrl:ensure_all_started(app1)),
    ?assertEqual({ok, []}, Ctrl:ensure_all_started(app1)),
    ?assertEqual(ok, Ctrl:ensure_all_stopped(ExpectedApps)),
    %% make sure same set of apps as before is running
    Running2 = lists:keyfind(running, 1, application_controller:info()),
    ?assertEqual(Running, Running2),

    ok = application:stop(app3),
    ok = application:stop(dist2),

    %% additional assertions for application startup time
    Reports = flush_log([]),
    ?assertEqual(length(ExpectedApps) * 2, length(Reports)), %% 3 apps started, 3 stopped
    %%
    ok.

loop() ->
    [{doc, "Tests a topology that has a loop"}].

loop(Config) when is_list(Config) ->
    Ctrl = ?config(ct_group, Config),
    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    %% loop: app1 -> [side, app2] -> app1
    create_app(Priv, app1, [side, app2], undefined),
    create_app(Priv, side, [], undefined),
    create_app(Priv, app2, [app1], undefined),
    Prev = lists:keyfind(running, 1, application_controller:info()),
    ?assertEqual({error, {cycle, [app1, app2, app1]}}, Ctrl:ensure_all_started(app1)),
    %% make sure no apps were started
    New = lists:keyfind(running, 1, application_controller:info()),
    ?assertEqual(Prev, New),
    %% standard application_controller can't deal with circular dependencies,
    %%  and uncommenting next line will make it hang
    %% ?assertEqual({ok, Apps}, application:ensure_all_started(app1)),
    ok.

distributed_loop() ->
    [{doc, "Tests that distributed loop is allowed"}].

distributed_loop(Config) when is_list(Config) ->
    Ctrl = ?config(ct_group, Config),
    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    %% loop: app1 -> app2 -> app1
    create_app(Priv, app1, [], [app2]),
    create_app(Priv, app2, [], [app1]),
    {ok, Started} = Ctrl:ensure_all_started(app1),
    %% make sure apps are indeed running
    {running, Running} = lists:keyfind(running, 1, application_controller:info()),
    ?assert(lists:keymember(app1, 1, Running)),
    ?assert(lists:keymember(app2, 1, Running)),
    Ctrl:ensure_all_stopped(Started).

critical_path() ->
    [{doc, "Tests critical path calculation (extended start)"}].

critical_path(Config) when is_list(Config) ->
    Ctrl = ?config(ct_group, Config),
    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    %% root(10) -> left(100)  -> short1 (10) /short2(10)
    %%             right (10) -> long1 (200) /short2(10)
    Topo = [{root, [left, right], 10}, {left, [short1, short2], 100},
        {right, [long1], 10}, {short1, [], 10},
        {short2, [], 10}, {long1, [short2], 200}],
    [create_app(Priv, App, Deps, undefined, Delay, []) || {App, Deps, Delay} <- Topo],
    %% verify critical path: root -> right -> long1
    {ok, DAG} = Ctrl:ensure_all_started(root, temporary, #{}),
    ?assertMatch({_, [{root, _}, {right, _}, {long1, _}, {short2, _}], _},
        Ctrl:critical_path(DAG)),
    Ctrl:ensure_all_stopped(maps:keys(DAG)).

find_path() ->
    [{doc, "Tests critical path finding"}].

find_path(Config) when is_list(Config) ->
    Now = erlang:monotonic_time(),
    Delta = erlang:convert_time_unit(100, millisecond, native),
    DAG1 = #{
        v1 =>#{local => [v2], start => Now, started => Now + Delta},
        v2 =>#{local => [v3], start => Now + Delta, started => Now + 2 * Delta},
        v3 =>#{local => [], start => Now + 2 * Delta, started => Now + 3 * Delta}
    },
    ?assertMatch({300, [{v1, 100}, {v2, 100}, {v3, 100}], _}, flexi:critical_path(DAG1)).

boot_node() ->
    [{doc, "Starts a separate Erlang node with new flexi as boot script"}].

boot_node(Config) when is_list(Config) ->
    Priv = filename:join(?config(priv_dir, Config), ?FUNCTION_NAME),
    RelName = atom_to_list(?FUNCTION_NAME),
    Apps = [{bottom, []}, {left, [bottom]}, {right, [bottom]}, {top, [left, right]}],
    [ok = create_app(Priv, App, Deps, undefined, 0, []) || {App, Deps} <- Apps],
    %% create release, only mentioning top app
    {ok, Boot} = flexi:make_release(Priv, RelName, "1.0.0", [top], [
        {vm_args_src, ["-sname ${ERL_NODE}"]},
        {sys_config_src, ["[{top, [{meaning_of_life, ${FOURTYTWO}}]}]."]}
    ]),
    %% here, some 'configure' step is expected to run, shaping release configuration
    %%  to fit what node expects
    NodeRoot = filename:join(Priv, "node"),
    Name = peer:random_name(?FUNCTION_NAME),
    ok = flexi:deploy(NodeRoot, Boot, #{"ERL_NODE" => Name, "FOURTYTWO" => "42"}),
    %% start the node with this release
    {ok, Peer, Node} = flexi:boot(NodeRoot, #{connection => standard_io}),
    %% check that node name & sys config are successful
    RemoteApps = peer:call(Peer, application, which_applications, []),
    Node = peer:call(Peer, erlang, node, []),
    ?assertEqual(Name, hd(string:lexemes(atom_to_list(Node), "@"))),
    FourtyTwo = peer:call(Peer, application, get_env, [top, meaning_of_life]),
    peer:stop(Peer),
    ExpectedApps = [App || {App, _Deps} <- Apps],
    ActualApps = [App || {App, _, _} <- RemoteApps, lists:member(App, ExpectedApps)],
    ?assertEqual(lists:sort(ExpectedApps), lists:sort(ActualApps), RemoteApps),
    ?assertEqual({ok, 42}, FourtyTwo),
    ok.
