const {
  EMAIL_PROVIDER,
  EMAIL_FROM,
  RESEND_API_KEY,
  SENDGRID_API_KEY,
  EMAIL_DRY_RUN
} = require('../config/runtime');

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function toHtmlBody(text, branding = {}) {
  const safe = escapeHtml(text).replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const brandName = String(branding?.brandName || '').trim();
  const logoDataUrl = String(branding?.logoDataUrl || '').trim();
  const logoUrl = String(branding?.logoUrl || '').trim();
  const logoSrc = logoDataUrl || logoUrl;
  const logoBlock = logoSrc
    ? `<div style="margin-bottom:12px;"><img src="${escapeHtml(logoSrc)}" alt="${escapeHtml(brandName || 'Business logo')}" style="max-height:64px;width:auto;height:auto;object-fit:contain;display:block;" /></div>`
    : '';
  const nameBlock = brandName ? `<div style="font-size:16px;font-weight:700;margin-bottom:10px;">${escapeHtml(brandName)}</div>` : '';
  return `<div style="font-family:Arial,sans-serif;line-height:1.45;white-space:pre-wrap;">${logoBlock}${nameBlock}${safe}</div>`;
}

function parseFromAddress(raw) {
  const value = String(raw || '').trim();
  if (!value) return { email: '', name: '' };
  const m = value.match(/^(.+?)\s*<([^>]+)>$/);
  if (m) {
    return { name: String(m[1] || '').trim().replace(/^"|"$/g, ''), email: String(m[2] || '').trim() };
  }
  return { email: value, name: '' };
}

function resolveProvider() {
  const raw = String(EMAIL_PROVIDER || '').trim().toLowerCase();
  if (raw === 'resend' || raw === 'sendgrid') return raw;
  if (RESEND_API_KEY) return 'resend';
  if (SENDGRID_API_KEY) return 'sendgrid';
  return '';
}

function isConfigured(provider) {
  if (!provider) return false;
  if (provider === 'resend') return Boolean(RESEND_API_KEY);
  if (provider === 'sendgrid') return Boolean(SENDGRID_API_KEY);
  return false;
}

async function sendViaResend({ from, to, subject, textBody, htmlBody }) {
  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from,
      to,
      subject,
      text: textBody,
      html: htmlBody
    })
  });
  if (!res.ok) {
    const raw = await res.text().catch(() => '');
    throw new Error(`Resend ${res.status}: ${raw || 'send failed'}`);
  }
}

async function sendViaSendgrid({ fromEmail, fromName, to, subject, textBody, htmlBody }) {
  const payload = {
    personalizations: [{ to: [{ email: to }] }],
    from: fromName ? { email: fromEmail, name: fromName } : { email: fromEmail },
    subject,
    content: [
      { type: 'text/plain', value: textBody },
      { type: 'text/html', value: htmlBody }
    ]
  };
  const res = await fetch('https://api.sendgrid.com/v3/mail/send', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${SENDGRID_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    const raw = await res.text().catch(() => '');
    throw new Error(`SendGrid ${res.status}: ${raw || 'send failed'}`);
  }
}

async function deliverCampaignEmail({
  provider,
  fromEmail,
  fromName,
  recipient,
  subject,
  body,
  branding = {}
}) {
  const to = String(recipient?.email || '').trim().toLowerCase();
  if (!to) throw new Error('Missing recipient email');
  const textBody = String(body || '').trim();
  const htmlBody = toHtmlBody(textBody, branding);
  if (provider === 'resend') {
    const from = fromName ? `${fromName} <${fromEmail}>` : fromEmail;
    return sendViaResend({ from, to, subject, textBody, htmlBody });
  }
  if (provider === 'sendgrid') {
    return sendViaSendgrid({ fromEmail, fromName, to, subject, textBody, htmlBody });
  }
  throw new Error('Unsupported email provider');
}

async function sendEmailCampaign({
  subject,
  body,
  recipients,
  fromEmailCandidate = '',
  fromNameCandidate = '',
  branding = {}
}) {
  const provider = resolveProvider();
  const configured = isConfigured(provider);
  if (!configured) {
    return {
      provider: provider || 'none',
      configured: false,
      attempted: 0,
      delivered: 0,
      failed: 0,
      failures: [{ email: '', error: 'Email provider is not configured. Set EMAIL_PROVIDER + API key env vars.' }]
    };
  }

  const envFrom = parseFromAddress(EMAIL_FROM);
  const fallbackFrom = parseFromAddress(fromEmailCandidate);
  const fromEmail = String(envFrom.email || fallbackFrom.email || '').trim();
  const fromName = String(envFrom.name || fromNameCandidate || fallbackFrom.name || '').trim();
  if (!fromEmail) {
    return {
      provider,
      configured: true,
      attempted: 0,
      delivered: 0,
      failed: 0,
      failures: [{ email: '', error: 'Missing EMAIL_FROM (or workspace business email).' }]
    };
  }

  const list = Array.isArray(recipients) ? recipients : [];
  const failures = [];
  let delivered = 0;
  if (EMAIL_DRY_RUN) {
    return {
      provider,
      configured: true,
      attempted: list.length,
      delivered: list.length,
      failed: 0,
      failures: []
    };
  }

  for (const recipient of list) {
    try {
      await deliverCampaignEmail({
        provider,
        fromEmail,
        fromName,
        recipient,
        subject,
        body,
        branding
      });
      delivered += 1;
    } catch (err) {
      failures.push({
        email: String(recipient?.email || '').trim().toLowerCase(),
        error: String(err?.message || 'send failed').slice(0, 300)
      });
    }
  }

  return {
    provider,
    configured: true,
    attempted: list.length,
    delivered,
    failed: failures.length,
    failures
  };
}

module.exports = {
  sendEmailCampaign
};
