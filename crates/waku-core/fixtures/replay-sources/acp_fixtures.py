#!/usr/bin/env python3
"""Builds the ACP-family replay fixtures (#48) from T3 Code's recorded Grok
ACP transcripts plus Waku's own frames.

Usage: python3 -I acp_fixtures.py <t3code checkout> <waku fixtures/replay dir>

No agent CLI runs: inbound frames come from T3's recordings (adapted) or are
written here from the ACP spec, and outbound frames are what Waku's ACP
driver sends. See ../replay/ACP_FIXTURES.md for every fixture's source.
"""

import copy
import json
import pathlib
import sys

T3_FIXTURES = "apps/server/src/orchestration-v2/testkit/fixtures"
GROK_VERSION = "1.0.41"  # the CLI T3 recorded (metadata.grokVersion)
GROK_SESSION = "00000000-0000-4000-8000-000000000001"
FORK_SESSION = "00000000-0000-4000-8000-000000000002"
GENERIC_SESSION = "acp-session-1"

FLAVORS = {
    "acp": {"program": "acp-agent", "args": [], "env": {}},
    "cursor": {"program": "cursor-agent", "args": ["acp"], "env": {}},
    "grok": {
        "program": "grok",
        "args": ["agent", "stdio"],
        "env": {"GROK_OAUTH2_REFERRER": "waku"},
    },
    "kimi": {"program": "kimi", "args": ["acp"], "env": {}},
    "fx": {"program": "fx", "args": ["acp"], "env": {}},
}

# A spec-only agent (ACP v1): `loadSession` and nothing unstable. Used for
# every non-Grok flavor; their own initialize payloads were never recorded.
GENERIC_INIT = {
    "protocolVersion": 1,
    "agentCapabilities": {
        "loadSession": True,
        "promptCapabilities": {"image": False, "audio": False, "embeddedContext": True},
        "mcpCapabilities": {"http": False, "sse": False},
    },
    "authMethods": [],
}


def t3_entries(root, scenario):
    path = pathlib.Path(root) / T3_FIXTURES / scenario / "grok_transcript.ndjson"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def rename(value):
    """T3's prompt ids become Waku's, everywhere in an inbound frame."""
    return json.loads(json.dumps(value).replace("t3-xai-prompt-", "waku-prompt-"))


class Transcript:
    def __init__(self, provider, scenario, source, synthetic=True, notes=None):
        self.provider = provider
        self.scenario = scenario
        self.entries = []
        self.next_id = {}
        self.agent_id = {}
        self.prompts = 0
        meta = {
            "recordedBy": "acp_fixtures.py",
            "source": source,
            "synthetic": synthetic,
            "ignore": ["/params/clientInfo/version"],
        }
        if notes:
            meta["notes"] = notes
        self.header = {
            "type": "transcript_start",
            "provider": provider,
            "protocol": "acp.stdio",
            "version": GROK_VERSION if provider == "grok" else "0.0.0",
            "scenario": scenario,
            "metadata": meta,
        }

    def _channel(self, entry, channel):
        if channel and channel != "main":
            entry["channel"] = channel
        return entry

    def out(self, frame, label, channel=None):
        self.entries.append(self._channel({"type": "expect_outbound", "label": label, "frame": frame}, channel))

    def inbound(self, frame, label=None, channel=None):
        entry = {"type": "emit_inbound"}
        label = label or frame.get("method")
        if label:
            entry["label"] = label
        entry["frame"] = frame
        self.entries.append(self._channel(entry, channel))

    def request(self, method, params, channel=None):
        key = channel or "main"
        rpc_id = self.next_id.get(key, 1)
        self.next_id[key] = rpc_id + 1
        self.out({"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params}, method, channel)
        return rpc_id

    def notify_out(self, method, params, channel=None):
        self.out({"jsonrpc": "2.0", "method": method, "params": params}, method, channel)

    def result(self, rpc_id, result, label=None, channel=None):
        self.inbound({"jsonrpc": "2.0", "id": rpc_id, "result": result}, label, channel)

    def error(self, rpc_id, error, label=None, channel=None):
        self.inbound({"jsonrpc": "2.0", "id": rpc_id, "error": error}, label, channel)

    def notify_in(self, method, params, channel=None):
        self.inbound({"jsonrpc": "2.0", "method": method, "params": params}, method, channel)

    def agent_request(self, method, params, channel=None):
        key = channel or "main"
        rpc_id = self.agent_id.get(key, 0)
        self.agent_id[key] = rpc_id + 1
        self.inbound({"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params}, method, channel)
        return rpc_id

    def respond_out(self, rpc_id, result, label, channel=None):
        self.out({"jsonrpc": "2.0", "id": rpc_id, "result": result}, label, channel)

    def exit(self, status="success", channel=None, exit_code=None):
        entry = {"type": "runtime_exit", "status": status}
        if exit_code is not None:
            entry["exitCode"] = exit_code
        self.entries.append(self._channel(entry, channel))

    def write(self, root):
        path = pathlib.Path(root) / self.scenario / f"{self.provider}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps(self.header, separators=(",", ":"))]
        lines += [json.dumps(entry, separators=(",", ":")) for entry in self.entries]
        path.write_text("\n".join(lines) + "\n")


class Agent:
    """One flavor's launch, handshake and prompt framing."""

    def __init__(self, t3_root, provider):
        self.provider = provider
        self.grok = provider == "grok"
        self.t3_root = t3_root
        self.session = GROK_SESSION if self.grok else GENERIC_SESSION

    def init_result(self):
        if self.grok:
            for entry in t3_entries(self.t3_root, "simple"):
                frame = entry.get("frame", {})
                if frame.get("kind") == "response" and frame.get("method") == "initialize":
                    return frame["result"]
        return GENERIC_INIT

    def init_params(self):
        caps = {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}
        if self.provider == "cursor":
            caps["_meta"] = {"parameterizedModelPicker": True}
        return {
            "protocolVersion": 1,
            "clientCapabilities": caps,
            "clientInfo": {"name": "waku", "version": "0.0.0"},
        }

    def start(self, t, channel=None):
        flavor = FLAVORS[self.provider]
        # Each agent process numbers its prompts from 1.
        t.prompts = 0
        t.out(
            {"type": "process_start", "program": flavor["program"], "args": flavor["args"], "env": flavor["env"]},
            "process_start",
            channel,
        )
        rpc_id = t.request("initialize", self.init_params(), channel)
        t.result(rpc_id, self.init_result(), "initialize", channel)

    def new_session(self, t, session=None, extra=None):
        rpc_id = t.request("session/new", {"cwd": "<workspace>", "mcpServers": []})
        result = {"sessionId": session or self.session}
        result.update(extra or {})
        t.result(rpc_id, result, "session/new")

    def prompt(self, t, text, session=None, channel=None):
        params = {"sessionId": session or self.session, "prompt": [{"type": "text", "text": text}]}
        if self.grok:
            t.prompts += 1
            prompt_id = f"waku-prompt-{t.prompts}"
            params["_meta"] = {"promptId": prompt_id, "requestId": prompt_id}
        return t.request("session/prompt", params, channel)

    def cancel(self, t, session=None):
        t.notify_out("session/cancel", {"sessionId": session or self.session})

    def complete(self, t, rpc_id, stop="end_turn", session=None):
        """Ends a prompt: Grok's `prompt_complete` first, then the response."""
        if self.grok:
            t.notify_in(
                "_x.ai/session/prompt_complete",
                {"sessionId": session or self.session, "promptId": f"waku-prompt-{t.prompts}", "stopReason": stop, "agentResult": None},
            )
        t.result(rpc_id, {"stopReason": stop}, "session/prompt")

    def update(self, t, update, session=None):
        t.notify_in("session/update", {"sessionId": session or self.session, "update": update})

    def text(self, t, text, session=None):
        self.update(t, {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": text}}, session)

    def thought(self, t, text, session=None):
        self.update(t, {"sessionUpdate": "agent_thought_chunk", "content": {"type": "text", "text": text}}, session)


def strip_xai(frame):
    """A Grok frame as a spec-only agent would send it: no `_x.ai`
    notifications and no `_meta`."""
    method = frame.get("method", "")
    if method.startswith("_x.ai") or method.startswith("x.ai"):
        return None
    frame = copy.deepcopy(frame)
    params = frame.get("params")
    if isinstance(params, dict):
        params.pop("_meta", None)
        if isinstance(params.get("update"), dict):
            params["update"].pop("_meta", None)
    if isinstance(frame.get("result"), dict):
        frame["result"].pop("_meta", None)
    return frame


def adapt_t3(t, agent, scenario, prompts, after_prompt=None, stop_at_close=True):
    """Replays a T3 Grok transcript's inbound frames through `t`, with
    Waku's outbound frames: `prompts` are the texts Waku sends, in order.

    The initialize and session/new exchanges are rewritten (the agent's
    `start` and `new_session`); T3's `session/close` and everything after it
    are dropped (Waku closes stdin instead). Responses are matched to Waku's
    request ids by method; agent→client requests get ids from 0, and
    `after_prompt(t, method, agent_id)` writes Waku's reply to one.
    """
    pending = {}
    prompt_texts = list(prompts)
    session = agent.session
    started = False
    for entry in t3_entries(agent.t3_root, scenario):
        kind = entry.get("type")
        frame = entry.get("frame", {})
        method = frame.get("method")
        if kind == "expect_outbound":
            if method == "initialize":
                agent.start(t)
                started = True
            elif method == "session/new":
                rpc_id = t.request("session/new", {"cwd": "<workspace>", "mcpServers": []})
                pending["session/new"] = rpc_id
            elif method == "session/prompt":
                session = frame["params"]["sessionId"] if agent.grok else agent.session
                pending["session/prompt"] = agent.prompt(t, prompt_texts.pop(0), session)
            elif method == "session/cancel":
                agent.cancel(t, session)
            elif method == "session/close" and stop_at_close:
                break
            elif frame.get("kind") == "response":
                after_prompt(t, method, pending.pop(("agent", method)))
            else:
                raise ValueError(f"unhandled outbound {method} in {scenario}")
        elif kind == "emit_inbound":
            if method in ("initialize",) and frame.get("kind") == "response":
                continue  # written by `start`
            frame = rename(frame)
            if not agent.grok:
                frame = strip_xai(frame)
                if frame is None:
                    continue
            if frame.get("kind") == "response":
                rpc_id = pending.pop(method)
                if "error" in frame:
                    t.error(rpc_id, frame["error"], method)
                else:
                    result = frame.get("result")
                    if method == "session/new" and not agent.grok:
                        result = {"sessionId": agent.session}
                    t.result(rpc_id, result, method)
            elif frame.get("kind") == "request":
                params = frame.get("params")
                pending[("agent", method)] = t.agent_request(method, params)
            elif frame.get("kind") == "notification":
                params = frame.get("params")
                if not agent.grok and isinstance(params, dict) and "sessionId" in params:
                    params = dict(params, sessionId=agent.session)
                t.notify_in(method, params)
    assert started, scenario
    assert not prompt_texts, f"unused prompts in {scenario}: {prompt_texts}"


# -- scenarios ------------------------------------------------------------------------------

INPUT_SIMPLE = "Respond with the following text: fixture simple ok"


def simple(agent, source):
    t = Transcript(agent.provider, "simple", source)
    adapt_t3(t, agent, "simple", [INPUT_SIMPLE])
    t.exit()
    return t


def multi_turn(agent, source):
    t = Transcript(agent.provider, "multi_turn", source)
    adapt_t3(t, agent, "multi_turn", ["first", "second"])
    t.exit()
    return t


READ_ONLY_TEXT = "Read package.json and tsconfig.json, then answer exactly: read only tool fixture complete"


def tool_call_read_only(agent, source):
    t = Transcript(agent.provider, "tool_call_read_only", source)
    adapt_t3(t, agent, "tool_call_read_only", [READ_ONLY_TEXT])
    t.exit()
    return t


APPROVAL_TEXT = "Create .codex-probe-write-action.txt with exactly this text: codex app-server approval fixture"


def tool_call_approval(agent, source):
    t = Transcript(agent.provider, "tool_call_approval", source)

    def reply(t, method, agent_id):
        assert method == "session/request_permission", method
        t.respond_out(agent_id, {"outcome": {"outcome": "selected", "optionId": "allow-once"}}, method)

    adapt_t3(t, agent, "tool_call_read_only_on_request", [APPROVAL_TEXT], after_prompt=reply)
    t.exit()
    return t


QUESTION_TEXT = "Ask whether this fixture should prefer strict schemas or UI flexibility, then respond exactly: plan questions fixture complete"


def plan_questions_grok(agent, source):
    t = Transcript(agent.provider, "plan_questions", source)

    def reply(t, method, agent_id):
        t.respond_out(
            agent_id,
            {
                "outcome": "accepted",
                "answers": {
                    "For this fixture, should the plan prefer strict schemas or UI flexibility?": ["Strict schemas (Recommended)"]
                },
            },
            method,
        )

    adapt_t3(t, agent, "plan_questions", [QUESTION_TEXT], after_prompt=reply)
    t.exit()
    return t


def plan_questions_cursor(agent, source):
    t = Transcript(agent.provider, "plan_questions", source, notes="Cursor's cursor/ask_question shape from Waku's v1 driver and its unit tests.")
    agent.start(t)
    agent.new_session(t)
    rpc_id = agent.prompt(t, QUESTION_TEXT)
    agent.update(t, {"sessionUpdate": "plan", "entries": [
        {"content": "Ask about schema strictness", "priority": "high", "status": "in_progress"},
        {"content": "Report the decision", "priority": "medium", "status": "pending"},
    ]})
    ask = t.agent_request("cursor/ask_question", {
        "toolCallId": "ask-1",
        "questions": [{
            "id": "schema_vs_ui_flexibility",
            "prompt": "For this fixture, should the plan prefer strict schemas or UI flexibility?",
            "options": [
                {"id": "strict", "label": "Strict schemas (Recommended)"},
                {"id": "flexible", "label": "UI flexibility"},
            ],
            "allowMultiple": False,
        }],
    })
    t.respond_out(ask, {"answers": {"schema_vs_ui_flexibility": "Strict schemas (Recommended)"}}, "cursor/ask_question")
    agent.update(t, {"sessionUpdate": "plan", "entries": [
        {"content": "Ask about schema strictness", "priority": "high", "status": "completed"},
        {"content": "Report the decision", "priority": "medium", "status": "completed"},
    ]})
    agent.text(t, "plan questions fixture complete")
    agent.complete(t, rpc_id)
    t.exit()
    return t


def turn_interrupt(agent, source):
    """T3's Grok steering transcript up to the cancel: the first prompt's
    cancelled half."""
    t = Transcript(agent.provider, "turn_interrupt", source)
    agent.start(t)
    agent.new_session(t)
    rpc_id = agent.prompt(t, "[slow] take your time")
    agent.thought(t, "Working")
    agent.cancel(t)
    agent.complete(t, rpc_id, "cancelled")
    t.exit()
    return t


def turn_interrupt_mid_tool(agent, source):
    t = Transcript(agent.provider, "turn_interrupt_mid_tool", source)
    agent.start(t)
    agent.new_session(t)
    rpc_id = agent.prompt(t, "Run sleep 30, then answer: interrupt fixture should not finish naturally")
    agent.update(t, {
        "sessionUpdate": "tool_call", "toolCallId": "call-sleep", "title": "sleep 30",
        "kind": "execute", "status": "pending", "rawInput": {"command": "sleep 30"},
    })
    agent.update(t, {"sessionUpdate": "tool_call_update", "toolCallId": "call-sleep", "status": "in_progress"})
    agent.cancel(t)
    agent.update(t, {"sessionUpdate": "tool_call_update", "toolCallId": "call-sleep", "status": "failed",
                     "content": [{"type": "content", "content": {"type": "text", "text": "Interrupted"}}]})
    agent.complete(t, rpc_id, "cancelled")
    t.exit()
    return t


def steering_restart(agent, source):
    t = Transcript(agent.provider, "steering_restart", source)
    prompts = ["[slow] start the refactor", "use tabs"]
    adapt_t3(t, agent, "message_steering", prompts)
    t.exit()
    return t


def provider_thread_resume(agent, source):
    t = Transcript(agent.provider, "provider_thread_resume", source)
    agent.start(t)
    agent.new_session(t)
    rpc_id = agent.prompt(t, "one")
    agent.text(t, "first answer")
    agent.complete(t, rpc_id)
    t.exit()  # unload closes stdin
    agent.start(t, "main#2")
    if agent.grok:
        rpc_id = t.request("session/resume", {"sessionId": agent.session, "cwd": "<workspace>"}, "main#2")
        t.result(rpc_id, {}, "session/resume", "main#2")
    else:
        rpc_id = t.request("session/load", {"sessionId": agent.session, "cwd": "<workspace>", "mcpServers": []}, "main#2")
        # The load replay is history, not a live turn.
        for update in [
            {"sessionUpdate": "user_message_chunk", "content": {"type": "text", "text": "one"}},
            {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "first answer"}},
        ]:
            t.notify_in("session/update", {"sessionId": agent.session, "update": update}, "main#2")
        t.result(rpc_id, {}, "session/load", "main#2")
    rpc_id = agent.prompt(t, "two", channel="main#2")
    t.notify_in("session/update", {"sessionId": agent.session, "update": {
        "sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "second answer"}}}, "main#2")
    if agent.grok:
        t.notify_in("_x.ai/session/prompt_complete", {
            "sessionId": agent.session, "promptId": f"waku-prompt-{t.prompts}", "stopReason": "end_turn", "agentResult": None}, "main#2")
    t.result(rpc_id, {"stopReason": "end_turn"}, "session/prompt", "main#2")
    t.exit(channel="main#2")
    return t


def grok_fork_process(agent, t, retained_source):
    """The side process on `fork-1` that copies a Grok session."""
    t.out({"type": "process_start", "program": "grok", "args": ["agent", "--always-approve", "--no-leader", "stdio"], "env": {}},
          "process_start", "fork-1")
    rpc_id = t.request("initialize", {
        "protocolVersion": 1,
        "clientCapabilities": {"fs": {"readTextFile": True, "writeTextFile": True}, "terminal": True},
    }, "fork-1")
    t.result(rpc_id, agent.init_result(), "initialize", "fork-1")
    rpc_id = t.request("_x.ai/session/fork", {
        "sourceSessionId": retained_source, "sourceCwd": "<workspace>", "newCwd": "<workspace>"}, "fork-1")
    t.result(rpc_id, {"newSessionId": FORK_SESSION}, "_x.ai/session/fork", "fork-1")
    t.exit(channel="fork-1")


def two_turns(agent, t):
    agent.start(t)
    agent.new_session(t)
    for text, answer in [("one", "first answer"), ("two", "second answer")]:
        rpc_id = agent.prompt(t, text)
        agent.text(t, answer)
        agent.complete(t, rpc_id)


def thread_rollback(agent, source):
    t = Transcript(agent.provider, "thread_rollback", source)
    two_turns(agent, t)
    grok_fork_process(agent, t, agent.session)
    rpc_id = t.request("session/resume", {"sessionId": FORK_SESSION, "cwd": "<workspace>"})
    t.result(rpc_id, {}, "session/resume")
    rpc_id = agent.prompt(t, "three", FORK_SESSION)
    agent.text(t, "third answer", FORK_SESSION)
    agent.complete(t, rpc_id, session=FORK_SESSION)
    t.exit()
    return t


def thread_fork_native(agent, source):
    t = Transcript(agent.provider, "thread_fork_native", source)
    two_turns(agent, t)
    grok_fork_process(agent, t, agent.session)
    agent.start(t, "main#2")
    rpc_id = t.request("session/resume", {"sessionId": FORK_SESSION, "cwd": "<workspace>"}, "main#2")
    t.result(rpc_id, {}, "session/resume", "main#2")
    rpc_id = agent.prompt(t, "in the fork", FORK_SESSION, "main#2")
    t.notify_in("session/update", {"sessionId": FORK_SESSION, "update": {
        "sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "forked answer"}}}, "main#2")
    t.notify_in("_x.ai/session/prompt_complete", {
        "sessionId": FORK_SESSION, "promptId": f"waku-prompt-{t.prompts}", "stopReason": "end_turn", "agentResult": None}, "main#2")
    t.result(rpc_id, {"stopReason": "end_turn"}, "session/prompt", "main#2")
    t.exit()
    t.exit(channel="main#2")
    return t


USAGE_MESSAGE = "You have reached your usage limit. Upgrade or wait for the limit to reset."


def usage_limit(agent, source):
    t = Transcript(agent.provider, "usage_limit", source)
    agent.start(t)
    agent.new_session(t)
    rpc_id = agent.prompt(t, "[usage-limit] go")
    if agent.grok:
        # T3's grok_prompt_error shape, with Grok's rate-limit stop reason
        # and JSON-RPC code (-32003, as T3's xaiAcpExtension classifies it).
        t.notify_in("_x.ai/session_notification", {"sessionId": agent.session, "update": {
            "sessionUpdate": "retry_state", "type": "failed", "error_type": "rate_limit", "message": USAGE_MESSAGE}})
        t.notify_in("_x.ai/session/prompt_complete", {
            "sessionId": agent.session, "promptId": "waku-prompt-1", "stopReason": "rate_limit", "agentResult": USAGE_MESSAGE})
        t.error(rpc_id, {"code": -32003, "message": "Rate limited", "data": {"message": USAGE_MESSAGE, "http_status": 429}}, "session/prompt")
    else:
        t.error(rpc_id, {"code": -32603, "message": "Internal error", "data": {
            "message": USAGE_MESSAGE, "http_status": 429, "resetAt": "2026-01-01T05:00:00Z"}}, "session/prompt")
    t.exit()
    return t


def runtime_exit_unexpected(agent, source):
    t = Transcript(agent.provider, "runtime_exit_unexpected", source)
    agent.start(t)
    agent.new_session(t)
    agent.prompt(t, "Respond with the following text: fixture crash")
    agent.text(t, "fixture")
    t.exit("error", exit_code=1)
    return t


T3_SOURCE = "t3code " + T3_FIXTURES + "/{}/grok_transcript.ndjson"

SCENARIOS = {
    # name: (builder, flavors, source)
    "simple": (simple, "all", T3_SOURCE.format("simple")),
    "multi_turn": (multi_turn, "all", T3_SOURCE.format("multi_turn")),
    "tool_call_read_only": (tool_call_read_only, "all", T3_SOURCE.format("tool_call_read_only")),
    "tool_call_approval": (tool_call_approval, "all", T3_SOURCE.format("tool_call_read_only_on_request")),
    "turn_interrupt": (turn_interrupt, "all", "written from T3 message_steering (cancel half) and the ACP spec"),
    "turn_interrupt_mid_tool": (turn_interrupt_mid_tool, "all", "written from the ACP spec (tool_call kinds, session/cancel)"),
    "steering_restart": (steering_restart, "all", T3_SOURCE.format("message_steering")),
    "provider_thread_resume": (provider_thread_resume, "all", "written from the ACP spec (session/resume for Grok, session/load elsewhere)"),
    "usage_limit": (usage_limit, "all", "T3 grok_prompt_error shape with Grok's rate_limit stop reason; generic: JSON-RPC error with http_status 429"),
    "runtime_exit_unexpected": (runtime_exit_unexpected, "all", "written: a crash (runtime_exit error) mid-turn"),
    "thread_rollback": (thread_rollback, ["grok"], "written from Waku's grok_session fork protocol (fork-1) and the ACP spec"),
    "thread_fork_native": (thread_fork_native, ["grok"], "written from Waku's grok_session fork protocol (fork-1) and the ACP spec"),
}


def write_inputs(out):
    """Scenario steps. New scenarios get a shared `input.json`; where the
    fake's shared steps cannot drive an ACP agent (an approval needs
    Supervised mode and the agent's own option id, a question its own id),
    `input.<provider>.json` overrides them."""
    out = pathlib.Path(out)

    def write(name, steps):
        (out / name).parent.mkdir(parents=True, exist_ok=True)
        (out / name).write_text(json.dumps({"steps": steps}, separators=(",", ":")) + "\n")

    write("tool_call_read_only/input.json", [{"type": "message", "text": READ_ONLY_TEXT}])
    write("turn_interrupt_mid_tool/input.json", [
        {"type": "message", "text": "Run sleep 30, then answer: interrupt fixture should not finish naturally"},
        {"type": "wait", "until": "tool_started"},
        {"type": "interrupt"},
        {"type": "wait", "until": "turn_terminal"},
    ])
    write("runtime_exit_unexpected/input.json", [
        {"type": "message", "text": "Respond with the following text: fixture crash"},
        {"type": "wait", "until": "turn_terminal"},
    ])
    for provider in FLAVORS:
        write(f"tool_call_approval/input.{provider}.json", [
            {"type": "runtime_mode", "mode": "ask"},
            {"type": "message", "text": APPROVAL_TEXT},
            {"type": "wait", "until": "request_pending"},
            {"type": "respond", "request": "first_pending", "option": "allow-once"},
            {"type": "wait", "until": "turn_terminal"},
        ])
    for provider in ["grok", "cursor"]:
        write(f"plan_questions/input.{provider}.json", [
            {"type": "message", "text": QUESTION_TEXT},
            {"type": "wait", "until": "request_pending"},
            {"type": "answer", "request": "first_pending", "answers": [
                {"questionId": "schema_vs_ui_flexibility", "answers": ["Strict schemas (Recommended)"]}]},
            {"type": "wait", "until": "turn_terminal"},
        ])


def write_grok_homes(out):
    """`grok.home/` seeds: the source session and the copy `_x.ai/session/fork`
    leaves on disk (in Grok's layout, `~/.grok/sessions/<workspace>/<id>/`),
    which the driver truncates to the retained turns."""
    def prompt_chat(index, text):
        return {"type": "user", "prompt_index": index, "content": text}

    def prompt_update(session, text):
        return {"method": "session/update", "params": {"sessionId": session, "update": {
            "sessionUpdate": "user_message_chunk", "content": {"type": "text", "text": text}}}}

    def answer_update(session, text):
        return {"method": "session/update", "params": {"sessionId": session, "update": {
            "sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": text}}}}

    for scenario in ["thread_rollback", "thread_fork_native"]:
        sessions = pathlib.Path(out) / scenario / "grok.home" / ".grok" / "sessions" / "%2Freplay%2Fworkspace"
        for session in [GROK_SESSION, FORK_SESSION]:
            directory = sessions / session
            directory.mkdir(parents=True, exist_ok=True)
            chat = [{"type": "system", "content": "Grok system prompt"}]
            updates = []
            for index, (text, answer) in enumerate([("one", "first answer"), ("two", "second answer")]):
                chat += [prompt_chat(index, text), {"type": "assistant", "content": answer}]
                updates += [prompt_update(session, text), answer_update(session, answer)]
            (directory / "chat_history.jsonl").write_text("".join(json.dumps(line) + "\n" for line in chat))
            (directory / "updates.jsonl").write_text("".join(json.dumps(line) + "\n" for line in updates))
            summary = {
                "info": {"id": session, "cwd": "/replay/workspace"},
                "generated_title": "Replay fixture",
                "num_messages": len(updates),
                "num_chat_messages": len(chat),
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            }
            (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


def main():
    t3_root, out = sys.argv[1], sys.argv[2]
    write_inputs(out)
    write_grok_homes(out)
    for name, (builder, flavors, source) in SCENARIOS.items():
        for provider in FLAVORS if flavors == "all" else flavors:
            builder(Agent(t3_root, provider), source).write(out)
    plan_questions_grok(Agent(t3_root, "grok"), T3_SOURCE.format("plan_questions")).write(out)
    plan_questions_cursor(Agent(t3_root, "cursor"), "written: cursor/ask_question per Waku's v1 Cursor driver").write(out)


if __name__ == "__main__":
    main()
