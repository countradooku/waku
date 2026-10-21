import { describe, expect, test } from 'bun:test'
import { importDaemonPathAttachment, importFiles, readAttachmentImage, type AttachmentClient } from './attachments'
import { fakeConnection } from './test-fixtures'

const stored = {
  reference: 'waku-attachment:one',
  path: '/home/me/.waku/attachments/one/logo.png',
  name: 'logo.png',
  isDir: false,
}

describe('attachments over v8', () => {
  test('imports a daemon-host path without sending file bytes', async () => {
    const connection = fakeConnection({ 'attachments.importPath': () => stored })
    const attachment = await importDaemonPathAttachment(connection as unknown as AttachmentClient, '/Users/me/Pictures/logo.png')

    expect(connection.calls).toEqual([{ method: 'attachments.importPath', params: { path: '/Users/me/Pictures/logo.png' } }])
    expect(attachment).toEqual({
      path: stored.path,
      mention: '/Users/me/Pictures/logo.png',
      name: 'logo.png',
      isDir: false,
      isImage: true,
      blobReference: 'waku-attachment:one',
    })
  })

  test('uploads a browser file as a blob, then imports it as a one-part attachment', async () => {
    const connection = fakeConnection({ 'attachments.import': () => ({ ...stored, name: 'notes.txt' }) })
    const file = new File(['hello'], 'notes.txt', { type: 'text/plain' })
    const [attachment] = await importFiles(connection as unknown as AttachmentClient, [file])

    expect(connection.calls[0]).toMatchObject({ method: 'blobs.put', params: { size: 5, name: 'notes.txt' } })
    expect(connection.calls[0]!.params.mime).toStartWith('text/plain')
    expect(connection.calls[0]!.params.sha256).toMatch(/^[0-9a-f]{64}$/)
    expect(connection.calls[1]).toEqual({
      method: 'attachments.import',
      params: { name: 'notes.txt', kind: 'file', parts: [{ relativePath: 'notes.txt', blob: 'waku-blob:abc.txt' }] },
    })
    expect(attachment?.isImage).toBe(false)
  })

  test('downloads an attachment image through blobDownload with its inner path', async () => {
    const connection = fakeConnection({
      blobDownload: () => ({ meta: { type: 'meta', mime: 'image/png', sha256: '', size: 3 }, bytes: new Uint8Array([1, 2, 3]) }),
    })
    const url = await readAttachmentImage(connection as unknown as AttachmentClient, {
      blobReference: stored.reference,
      path: stored.path,
      name: stored.name,
    })
    expect(connection.downloads).toEqual([{ reference: stored.reference, path: stored.path }])
    expect(url).toBe('data:image/png;base64,AQID')
  })
})
