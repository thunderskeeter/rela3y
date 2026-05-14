function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function pickString(value) {
  const text = String(value || '').trim();
  return text || null;
}

function pickNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function buildMessagePayloadProjection(message = {}) {
  const meta = asObject(message.meta);
  const attachments = asArray(message.attachments);
  const payload = {};

  if (Object.keys(meta).length) payload.meta = meta;
  if (attachments.length) payload.attachments = attachments;

  return payload;
}

function buildConversationPayloadProjection(conversation = {}) {
  return {};
}

module.exports = {
  buildMessagePayloadProjection,
  buildConversationPayloadProjection
};
