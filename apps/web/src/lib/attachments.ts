// Attachments over protocol v8 (RFC 0003 §5.6): a browser file is uploaded
// as a blob (`blobs.put` + DATA) and then imported (`attachments.import`); a
// daemon-host path is imported in place (`attachments.importPath`). Images
// are downloaded through the `blobDownload` stream.

import type { DaemonConnection } from '@waku/client/v8'
import type * as T from '@waku/client/v8'

/** The attachment store's per-file cap. */
const MAX_UPLOAD_BYTES = 32 * 1024 * 1024

export type AttachmentClient = Pick<DaemonConnection, 'uploadBlob' | 'call' | 'downloadBlob'>

export async function importFiles(
  connection: AttachmentClient,
  files: File[],
): Promise<T.Attachment[]> {
  const attachments: T.Attachment[] = []
  for (const file of files) {
    if (file.size > MAX_UPLOAD_BYTES) {
      throw new Error(`${file.name} is too large to send to the daemon`)
    }
    const bytes = new Uint8Array(await file.arrayBuffer())
    const blob = await connection.uploadBlob(bytes, {
      mime: file.type || 'application/octet-stream',
      name: file.name,
      sha256: await sha256Hex(bytes),
    })
    const stored = await connection.call('attachments.import', {
      name: file.name,
      kind: 'file',
      parts: [{ relativePath: file.name, blob: blob.reference }],
    })
    attachments.push({
      path: stored.path,
      mention: stored.path,
      name: stored.name,
      isDir: stored.isDir,
      isImage: file.type.startsWith('image/') || isImageName(file.name),
      blobReference: stored.reference,
    })
  }
  return attachments
}

export async function importDaemonPathAttachment(
  connection: AttachmentClient,
  path: string,
): Promise<T.Attachment> {
  const stored = await connection.call('attachments.importPath', { path })
  const mention = stored.isDir && !path.endsWith('/') ? `${path}/` : path
  return {
    path: stored.path,
    mention,
    name: stored.name,
    isDir: stored.isDir,
    isImage: !stored.isDir && isImageName(stored.name),
    blobReference: stored.reference,
  }
}

/** A data URL for an image attachment or a `waku-blob:` image reference. */
export async function readAttachmentImage(
  connection: AttachmentClient,
  attachment: Pick<T.Attachment, 'blobReference' | 'path' | 'name'>,
): Promise<string> {
  const reference = attachment.blobReference
  if (!reference) throw new Error('This attachment has no daemon reference')
  const { meta, bytes } = reference.startsWith('waku-blob:')
    ? await connection.downloadBlob(reference)
    : await connection.downloadBlob(reference, attachment.path)
  const mime = meta.type === 'meta' && meta.mime.startsWith('image/') ? meta.mime : imageMimeType(attachment.name)
  return `data:${mime};base64,${encodeBase64(bytes)}`
}

/** Draft attachments carry the same fields with the flags required. */
export function draftAttachment(attachment: T.Attachment): T.DraftAttachment {
  return {
    ...attachment,
    isDir: attachment.isDir ?? false,
    isImage: attachment.isImage ?? false,
  }
}

function imageMimeType(name: string): string {
  const extension = name.split('.').at(-1)?.toLowerCase()
  return (
    {
      avif: 'image/avif',
      gif: 'image/gif',
      heic: 'image/heic',
      jpeg: 'image/jpeg',
      jpg: 'image/jpeg',
      png: 'image/png',
      svg: 'image/svg+xml',
      webp: 'image/webp',
    }[extension ?? ''] ?? 'application/octet-stream'
  )
}

async function sha256Hex(bytes: Uint8Array): Promise<string | undefined> {
  if (!globalThis.crypto?.subtle) return undefined
  const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes as Uint8Array<ArrayBuffer>)
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('')
}

export function encodeBase64(bytes: Uint8Array): string {
  let binary = ''
  for (let index = 0; index < bytes.length; index += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(index, index + 0x8000))
  }
  return btoa(binary)
}

export function isImageName(name: string): boolean {
  return ['avif', 'gif', 'heic', 'jpeg', 'jpg', 'png', 'svg', 'webp'].includes(
    name.split('.').at(-1)?.toLowerCase() ?? '',
  )
}
