import { BottomSheetFlatList } from "@expo/ui/community/bottom-sheet";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  inspectGitStatus,
  isGitFileStaged,
  pullGitBranch,
  stageGitFiles,
  type GitStatusFile,
} from "@waku/client";
import { useRef, useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  Text,
  TextInput,
  View,
} from "react-native";
import { useTheme } from "@/hooks/use-theme";
import { useDaemon } from "@/lib/daemon-context";

export function GitSurface({ root }: { root: string | null }) {
  const daemon = useDaemon();
  const theme = useTheme();
  const queryClient = useQueryClient();
  const [pending, setPending] = useState(false);
  const lock = useRef(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState<string | null>(null);
  const profile = daemon.activeProfile?.id ?? "disconnected";
  const status = useQuery({
    queryKey: ["daemon", profile, "git-status", root],
    queryFn: () => inspectGitStatus(daemon.client!, root!),
    enabled: Boolean(root && daemon.client && daemon.phase === "connected"),
  });
  const connected = daemon.phase === "connected";
  const disabled =
    pending ||
    status.isFetching ||
    status.isError ||
    !status.data ||
    !connected;

  async function run(operation: () => Promise<void>) {
    if (lock.current || !connected) return;
    lock.current = true;
    setPending(true);
    setError(null);
    try {
      await operation();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      await queryClient.invalidateQueries({ queryKey: ["daemon", profile] });
      lock.current = false;
      setPending(false);
    }
  }

  async function commit() {
    const response = await daemon.client!.request({
      type: "workspace",
      operation: {
        type: "commit",
        cwd: root!,
        message: message.trim(),
        include_unstaged: false,
        push: false,
      },
    });
    if (response.type !== "workspace" || response.result.type !== "ack")
      throw new Error("Unexpected commit response");
    setMessage("");
  }

  function button(
    label: string,
    enabled: boolean,
    action: () => Promise<void>,
  ) {
    return (
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ disabled: !enabled }}
        disabled={!enabled}
        onPress={() => void run(action)}
        style={{ padding: 12, opacity: enabled ? 1 : 0.4 }}
      >
        <Text style={{ color: theme.text }}>{label}</Text>
      </Pressable>
    );
  }

  if (!root)
    return (
      <Text style={{ padding: 16, color: theme.text }}>
        This task has no workspace.
      </Text>
    );
  return (
    <View style={{ flex: 1 }}>
      <Text style={{ padding: 16, fontSize: 18, color: theme.text }}>
        Source control · {status.data?.branch ?? "HEAD"}
      </Text>
      {status.data?.upstream && (
        <Text style={{ paddingHorizontal: 16, color: theme.textSecondary }}>
          {status.data.upstream} · ↑{status.data.ahead} ↓{status.data.behind}
        </Text>
      )}
      {(error || status.error) && (
        <Text
          accessibilityRole="alert"
          style={{ padding: 16, color: theme.danger }}
        >
          {error ?? String(status.error)}
        </Text>
      )}
      {(pending || status.isFetching) && <ActivityIndicator />}
      <BottomSheetFlatList<GitStatusFile>
        data={status.data?.files ?? []}
        keyExtractor={(file) => file.path}
        ListEmptyComponent={
          <Text style={{ padding: 16, color: theme.textSecondary }}>
            {status.data ? "Working tree clean" : "Connecting…"}
          </Text>
        }
        renderItem={({ item }) => {
          const staged = isGitFileStaged(item.index_status);
          return (
            <Pressable
              accessibilityRole="checkbox"
              accessibilityState={{ checked: staged, disabled }}
              accessibilityLabel={`${staged ? "Unstage" : "Stage"} ${item.path}`}
              disabled={disabled}
              onPress={() =>
                void run(() =>
                  stageGitFiles(daemon.client!, root, [item.path], !staged),
                )
              }
              style={{ padding: 16, flexDirection: "row", gap: 12 }}
            >
              <Text style={{ color: theme.text }}>{staged ? "☑" : "☐"}</Text>
              <Text numberOfLines={1} style={{ color: theme.text, flex: 1 }}>
                {item.path}
              </Text>
              <Text style={{ color: theme.textSecondary }}>
                {item.index_status}
                {item.worktree_status}
              </Text>
            </Pressable>
          );
        }}
      />
      <TextInput
        accessibilityLabel="Commit message"
        placeholder="Commit message"
        placeholderTextColor={theme.textSecondary}
        editable={!pending}
        value={message}
        onChangeText={setMessage}
        style={{
          padding: 16,
          color: theme.text,
          borderTopWidth: 1,
          borderColor: theme.border,
        }}
      />
      {button(
        "Commit staged files",
        !disabled &&
          Boolean(message.trim()) &&
          Boolean(
            status.data?.files.some((file) =>
              isGitFileStaged(file.index_status),
            ),
          ),
        commit,
      )}
      {button(
        "Pull (fast-forward only)",
        !disabled &&
          Boolean(
            status.data?.branch &&
              status.data.upstream &&
              status.data.files.length === 0,
          ),
        () => pullGitBranch(daemon.client!, root),
      )}
      {button("Refresh", !pending && connected, async () => {
        await status.refetch();
      })}
    </View>
  );
}
