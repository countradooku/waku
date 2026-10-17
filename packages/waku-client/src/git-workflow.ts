import type { WakuClient } from "./client";
import type { GitStatus, GitWorktree } from "./generated";

export async function inspectGitStatus(
  client: WakuClient,
  cwd: string,
): Promise<GitStatus> {
  const response = await client.request({
    type: "workspace",
    operation: { type: "gitStatus", cwd },
  });
  if (response.type !== "workspace" || response.result.type !== "gitStatus") {
    throw new Error("The daemon returned an unexpected Git status response");
  }
  return response.result.status;
}

export async function stageGitFiles(
  client: WakuClient,
  cwd: string,
  paths: string[],
  stage: boolean,
): Promise<void> {
  const response = await client.request({
    type: "workspace",
    operation: { type: "stageFiles", cwd, paths, stage },
  });
  if (response.type !== "workspace" || response.result.type !== "ack") {
    throw new Error("The daemon returned an unexpected staging response");
  }
}

export async function pullGitBranch(
  client: WakuClient,
  cwd: string,
): Promise<void> {
  const response = await client.request({
    type: "workspace",
    operation: { type: "pull", cwd },
  });
  if (response.type !== "workspace" || response.result.type !== "ack") {
    throw new Error("The daemon returned an unexpected pull response");
  }
}

export async function listGitWorktrees(
  client: WakuClient,
  cwd: string,
): Promise<GitWorktree[]> {
  const response = await client.request({
    type: "workspace",
    operation: { type: "listWorktrees", cwd },
  });
  if (response.type !== "workspace" || response.result.type !== "worktrees") {
    throw new Error("The daemon returned an unexpected worktree response");
  }
  return response.result.worktrees;
}

export function isGitFileStaged(status: string): boolean {
  return status !== "." && status !== "?";
}
