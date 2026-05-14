const token = new URLSearchParams(window.location.search || '').get('token') || '';
const signedInPanel = document.getElementById('qrSignedInPanel');
const loginForm = document.getElementById('qrLoginForm');
const donePanel = document.getElementById('qrDonePanel');
const statusEl = document.getElementById('qrStatus');
const userPill = document.getElementById('qrUserPill');
const approveBtn = document.getElementById('qrApproveBtn');
const loginApproveBtn = document.getElementById('qrLoginApproveBtn');
const qrLead = document.getElementById('qrLead');

const setStatus = (message, kind = '') => {
  statusEl.textContent = message || '';
  statusEl.className = 'status' + (kind ? ' ' + kind : '');
};

const setMode = (mode, currentUser = null) => {
  signedInPanel.classList.toggle('hidden', mode !== 'signed-in');
  loginForm.classList.toggle('hidden', mode !== 'login');
  donePanel.classList.toggle('hidden', mode !== 'done');
  if (currentUser && userPill) {
    userPill.textContent = (currentUser.name || currentUser.email || 'Signed in') + ' ? ' + (currentUser.role || 'user');
  }
};

async function fetchJson(url, init = {}) {
  const res = await fetch(url, {
    credentials: 'include',
    ...init,
    headers: {
      ...(init.headers || {}),
      'Content-Type': 'application/json'
    }
  });
  const payload = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(String(payload?.error || ('Request failed (' + res.status + ')')));
  return payload;
}

async function loadState() {
  if (!token) {
    qrLead.textContent = 'This QR sign-in link is missing a token.';
    setMode('done');
    setStatus('QR token missing.', 'error');
    return;
  }
  try {
    const payload = await fetchJson('/api/auth/qr/inspect?token=' + encodeURIComponent(token), { method: 'GET' });
    if (payload.status === 'approved' || payload.status === 'completed') {
      setMode('done');
      setStatus('Desktop sign-in already approved.', 'success');
      return;
    }
    if (payload.currentUser) {
      setMode('signed-in', payload.currentUser);
      setStatus('Ready to approve this desktop sign-in.');
      return;
    }
    setMode('login');
    setStatus('Sign in on your phone to approve this desktop request.');
  } catch (err) {
    setMode('done');
    setStatus(err?.message || 'Failed to inspect QR request.', 'error');
  }
}

async function approve(body) {
  setStatus('Approving sign-in...');
  approveBtn.disabled = true;
  loginApproveBtn.disabled = true;
  try {
    await fetchJson('/api/auth/qr/approve', {
      method: 'POST',
      body: JSON.stringify(body)
    });
    setMode('done');
    setStatus('Desktop sign-in approved. You can return to your computer now.', 'success');
  } catch (err) {
    setStatus(err?.message || 'Failed to approve sign-in.', 'error');
  } finally {
    approveBtn.disabled = false;
    loginApproveBtn.disabled = false;
  }
}

if (approveBtn) {
  approveBtn.addEventListener('click', () => {
    approve({ token });
  });
}

if (loginForm) {
  loginForm.addEventListener('submit', (event) => {
    event.preventDefault();
    approve({
      token,
      email: document.getElementById('qrEmail').value.trim(),
      password: document.getElementById('qrPassword').value,
      persist: true
    });
  });
}

loadState();
