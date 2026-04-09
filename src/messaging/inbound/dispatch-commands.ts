/**
 * Copyright (c) 2026 ByteDance Ltd. and/or its affiliates
 * SPDX-License-Identifier: MIT
 *
 * System command and permission notification dispatch for inbound messages.
 *
 * Handles control commands (/help, /reset, etc.) via plain-text delivery
 * and permission-error notifications via the streaming card flow.
 */

import type { LarkClient } from '../../core/lark-client';
import { larkLogger } from '../../core/lark-logger';
import { ticketElapsed } from '../../core/lark-ticket';
import { createFeishuReplyDispatcher } from '../../card/reply-dispatcher';
import { startToolUseTraceRun } from '../../card/tool-use-trace-store';
import { sendMessageFeishu } from '../outbound/send';
import type { PermissionError } from './permission';
import type { DispatchContext } from './dispatch-context';
import { buildInboundPayload } from './dispatch-builders';

const log = larkLogger('inbound/dispatch-commands');
const SYSTEM_COMMAND_SYNC_BUDGET_MS = 1200;
const SYSTEM_COMMAND_BACKGROUND_HARD_TIMEOUT_MS = 2000;

// ---------------------------------------------------------------------------
// Permission error notification
// ---------------------------------------------------------------------------

/**
 * Dispatch a permission-error notification to the agent so it can
 * inform the user about the missing Feishu API scope.
 */
export async function dispatchPermissionNotification(
  dc: DispatchContext,
  permissionError: PermissionError,
  replyToMessageId?: string,
): Promise<void> {
  const grantUrl = permissionError.grantUrl ?? '';
  const permissionNotifyBody = `[System: The bot encountered a Feishu API permission error. Please inform the user about this issue and provide the permission grant URL for the admin to authorize. Permission grant URL: ${grantUrl}]`;

  const permBody = dc.core.channel.reply.formatAgentEnvelope({
    channel: 'Feishu',
    from: dc.envelopeFrom,
    timestamp: new Date(),
    envelope: dc.envelopeOptions,
    body: permissionNotifyBody,
  });

  const permCtx = buildInboundPayload(dc, {
    body: permBody,
    bodyForAgent: permissionNotifyBody,
    rawBody: permissionNotifyBody,
    commandBody: permissionNotifyBody,
    senderName: 'system',
    senderId: 'system',
    messageSid: `${dc.ctx.messageId}:permission-error`,
    wasMentioned: false,
  });

  startToolUseTraceRun(dc.threadSessionKey ?? dc.route.sessionKey);

  const {
    dispatcher: permDispatcher,
    replyOptions: permReplyOptions,
    markDispatchIdle: markPermIdle,
    markFullyComplete: markPermComplete,
  } = createFeishuReplyDispatcher({
    cfg: dc.accountScopedCfg,
    agentId: dc.route.agentId,
    chatId: dc.ctx.chatId,
    sessionKey: dc.threadSessionKey ?? dc.route.sessionKey,
    replyToMessageId: replyToMessageId ?? dc.ctx.messageId,
    accountId: dc.account.accountId,
    chatType: dc.ctx.chatType,
    replyInThread: dc.isThread,
    toolUseDisplay: {
      mode: 'off',
      showToolUse: false,
      showToolResultDetails: false,
      showFullPaths: false,
    },
  });

  dc.log(`feishu[${dc.account.accountId}]: dispatching permission error notification to agent`);

  await dc.core.channel.reply.dispatchReplyFromConfig({
    ctx: permCtx,
    cfg: dc.accountScopedCfg,
    dispatcher: permDispatcher,
    replyOptions: permReplyOptions,
  });

  await permDispatcher.waitForIdle();
  markPermComplete();
  markPermIdle();
}

// ---------------------------------------------------------------------------
// System command dispatch
// ---------------------------------------------------------------------------

/**
 * Dispatch a system command (/help, /reset, etc.) via plain-text delivery.
 * No streaming card, no "Processing..." state.
 */
export async function dispatchSystemCommand(
  dc: DispatchContext,
  ctxPayload: ReturnType<typeof LarkClient.runtime.channel.reply.finalizeInboundContext>,
  replyToMessageId?: string,
): Promise<void> {
  let delivered = false;
  let settled = false;
  let hardTimeoutTimer: ReturnType<typeof setTimeout> | undefined;

  const suppressToolDetails = isLifecycleSessionCommand(dc.ctx.content);
  const abortController = suppressToolDetails ? new AbortController() : undefined;

  dc.log(
    `feishu[${dc.account.accountId}]: detected system command, using plain-text dispatch`,
  );
  log.info('system command detected, plain-text dispatch');

  const runDispatch = async (): Promise<void> => {
    await dc.core.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
      ctx: ctxPayload,
      cfg: dc.accountScopedCfg,
      dispatcherOptions: {
        deliver: async (payload, info) => {
          if (suppressToolDetails && info.kind === 'tool') {
            return;
          }

          const text = payload.text?.trim() ?? '';
          if (!text) return;
          await sendMessageFeishu({
            cfg: dc.accountScopedCfg,
            to: dc.ctx.chatId,
            text,
            replyToMessageId: replyToMessageId ?? dc.ctx.messageId,
            accountId: dc.account.accountId,
            replyInThread: dc.isThread,
          });
          delivered = true;
        },
        onSkip: (_payload, info) => {
          if (info.reason !== 'silent') {
            dc.log(`feishu[${dc.account.accountId}]: command reply skipped (reason=${info.reason})`);
          }
        },
        onError: (err, info) => {
          dc.error(`feishu[${dc.account.accountId}]: command ${info.kind} reply failed: ${String(err)}`);
        },
      },
      replyOptions: abortController ? { abortSignal: abortController.signal } : {},
    });

    dc.log(`feishu[${dc.account.accountId}]: system command dispatched (delivered=${delivered})`);
    log.info(`system command dispatched (delivered=${delivered}, elapsed=${ticketElapsed()}ms)`);
  };

  if (abortController) {
    hardTimeoutTimer = setTimeout(() => {
      if (settled) return;
      abortController.abort();
      dc.log(
        `feishu[${dc.account.accountId}]: system command background hard-timeout abort (timeout_ms=${SYSTEM_COMMAND_BACKGROUND_HARD_TIMEOUT_MS})`,
      );
      log.warn(
        `system command background hard-timeout abort (timeout_ms=${SYSTEM_COMMAND_BACKGROUND_HARD_TIMEOUT_MS})`,
      );
    }, SYSTEM_COMMAND_BACKGROUND_HARD_TIMEOUT_MS);
  }

  const guardedDispatch = runDispatch()
    .catch((err) => {
      if (isLikelyAbortError(err)) {
        dc.log(`feishu[${dc.account.accountId}]: system command dispatch aborted`);
        log.warn('system command dispatch aborted');
        return;
      }
      dc.error(`feishu[${dc.account.accountId}]: system command dispatch failed: ${String(err)}`);
    })
    .finally(() => {
      settled = true;
      if (hardTimeoutTimer) clearTimeout(hardTimeoutTimer);
    });

  if (!suppressToolDetails) {
    await guardedDispatch;
    return;
  }

  const completedWithinBudget = await Promise.race([
    guardedDispatch.then(() => true),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(false), SYSTEM_COMMAND_SYNC_BUDGET_MS)),
  ]);

  if (!completedWithinBudget) {
    dc.log(
      `feishu[${dc.account.accountId}]: system command detached from queue (budget_ms=${SYSTEM_COMMAND_SYNC_BUDGET_MS})`,
    );
    log.warn(`system command detached from queue (budget_ms=${SYSTEM_COMMAND_SYNC_BUDGET_MS})`);
  }
}

function isLifecycleSessionCommand(text: string | undefined): boolean {
  if (!text) return false;
  const match = text.trim().match(/^\/([^\s@]+)/);
  if (!match) return false;
  const command = match[1]?.toLowerCase();
  return command === 'new' || command === 'reset';
}

function isLikelyAbortError(err: unknown): boolean {
  if (err instanceof Error) {
    const name = err.name.toLowerCase();
    const msg = err.message.toLowerCase();
    return name.includes('abort') || msg.includes('abort');
  }
  const text = String(err).toLowerCase();
  return text.includes('abort');
}
