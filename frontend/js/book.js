(function () {
  const token = (() => {
    const p = String(window.location.pathname || '').split('/').filter(Boolean);
    if (p[0] === 'book' && p[1]) return decodeURIComponent(p[1]);
    return '';
  })();

  const bizNameEl = document.getElementById('bizName');
  const subLineEl = document.getElementById('subLine');
  const dateEl = document.getElementById('date');
  const daysEl = document.getElementById('days');
  const slotsEl = document.getElementById('slots');
  const slotMsgEl = document.getElementById('slotMsg');
  const summaryEl = document.getElementById('bookingSummary');
  const nameEl = document.getElementById('name');
  const phoneEl = document.getElementById('phone');
  const emailEl = document.getElementById('email');
  const bookBtn = document.getElementById('bookBtn');
  const rescheduleBtn = document.getElementById('rescheduleBtn');
  const cancelBtn = document.getElementById('cancelBtn');
  const manageMsgEl = document.getElementById('manageMsg');
  const statusEl = document.getElementById('status');
  const searchParams = new URLSearchParams(window.location.search);
  const manageToken = searchParams.get('manage') || '';
  const summaryFromQuery = String(
    searchParams.get('summary')
    || searchParams.get('request')
    || searchParams.get('intent')
    || searchParams.get('service')
    || ''
  ).trim();
  const serviceIdFromQuery = String(searchParams.get('serviceId') || '').trim();
  const serviceNameFromQuery = String(searchParams.get('serviceName') || '').trim();

  const state = {
    services: [],
    timezone: 'America/New_York',
    selectedSlot: null,
    selectedServiceId: '',
    selectedServiceName: '',
    managedBookingId: '',
    dayOnly: false,
    availabilityDays: []
  };

  function api(path, opts) {
    return fetch(`/api/public${path}`, opts).then(async (res) => {
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body?.error || `HTTP ${res.status}`);
      return body;
    });
  }

  function toDateInput(ms) {
    const d = new Date(ms || Date.now());
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  }

  function setStatus(msg, isErr) {
    statusEl.textContent = msg || '';
    statusEl.style.color = isErr ? '#ffb4b4' : '';
  }

  function renderPostBookingLinks({ manageUrl = '', paymentUrl = '' } = {}) {
    if (!manageMsgEl) return;
    manageMsgEl.innerHTML = '';
    const links = [];
    const fullManageUrl = manageUrl ? `${window.location.origin}${manageUrl}` : '';
    if (fullManageUrl) links.push({ label: 'Manage booking', url: fullManageUrl });
    if (paymentUrl) links.push({ label: 'Pay invoice', url: paymentUrl, primary: true });
    if (!links.length) {
      manageMsgEl.classList.add('hidden');
      return;
    }
    manageMsgEl.classList.remove('hidden');
    for (const link of links) {
      const a = document.createElement('a');
      a.href = link.url;
      a.textContent = link.label;
      a.target = '_blank';
      a.rel = 'noopener noreferrer';
      a.style.display = 'inline-flex';
      a.style.marginRight = '10px';
      a.style.marginTop = '8px';
      a.style.color = link.primary ? '#b4dbff' : 'inherit';
      a.style.fontWeight = link.primary ? '700' : '600';
      manageMsgEl.appendChild(a);
    }
  }

  function renderSummary() {
    const parts = [];
    if (summaryFromQuery) parts.push(summaryFromQuery);
    if (!parts.length && state.selectedServiceName) parts.push(state.selectedServiceName);
    const text = parts.length
      ? parts.join(' ')
      : 'Service details already captured. Confirm date and contact info to book.';
    if (summaryEl) summaryEl.textContent = text;
  }

  function renderSlots(items) {
    slotsEl.innerHTML = '';
    state.selectedSlot = null;
    if (state.dayOnly) {
      slotsEl.classList.add('hidden');
      return;
    }
    slotsEl.classList.remove('hidden');
    if (!items.length) {
      slotMsgEl.textContent = 'No open slots for this date.';
      return;
    }
    slotMsgEl.textContent = `Available (${state.timezone})`;
    for (const item of items) {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'slot';
      btn.textContent = String(item.label || '');
      btn.addEventListener('click', () => {
        state.selectedSlot = item;
        Array.from(slotsEl.querySelectorAll('.slot')).forEach((el) => el.classList.remove('active'));
        btn.classList.add('active');
      });
      slotsEl.appendChild(btn);
    }
  }

  function renderDays(days) {
    state.availabilityDays = Array.isArray(days) ? days : [];
    daysEl.innerHTML = '';
    if (!state.availabilityDays.length) return;
    const dows = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    for (const d of dows) {
      const h = document.createElement('div');
      h.className = 'day-dow';
      h.textContent = d;
      daysEl.appendChild(h);
    }
    const firstIso = String(state.availabilityDays[0]?.date || '');
    const firstDate = firstIso ? new Date(`${firstIso}T00:00:00`) : null;
    const lead = Number.isFinite(firstDate?.getTime?.()) ? firstDate.getDay() : 0;
    for (let i = 0; i < lead; i += 1) {
      const empty = document.createElement('button');
      empty.type = 'button';
      empty.className = 'day-pill day-empty';
      empty.disabled = true;
      empty.setAttribute('aria-hidden', 'true');
      daysEl.appendChild(empty);
    }
    for (const day of state.availabilityDays) {
      const slots = Array.isArray(day?.slots) ? day.slots : [];
      const isClosed = day?.closed === true || String(day?.status || '').toLowerCase() === 'closed';
      const isFull = !isClosed && (day?.full === true || slots.length === 0);
      const isUnavailable = isClosed || isFull;
      const d = new Date(`${String(day.date || '')}T00:00:00`);
      const top = Number.isFinite(d.getTime())
        ? d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
        : String(day.date || '');
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = `day-pill ${isUnavailable ? 'unavailable' : ''}`;
      btn.disabled = isUnavailable;
      const label = isClosed ? 'Closed' : (isFull ? 'Full' : `${slots.length} open`);
      btn.innerHTML = `<span>${top}</span><small>${label}</small>`;
      if (String(dateEl.value) === String(day.date || '')) btn.classList.add('active');
      if (!isUnavailable) btn.addEventListener('click', () => {
        dateEl.value = String(day.date || '');
        Array.from(daysEl.querySelectorAll('.day-pill')).forEach((el) => el.classList.remove('active'));
        btn.classList.add('active');
        if (state.dayOnly) {
          state.selectedSlot = slots[0] || null;
          slotMsgEl.textContent = slots.length
            ? `Open start day selected (${state.timezone})`
            : 'That day is full.';
          return;
        }
        renderSlots(slots);
      });
      daysEl.appendChild(btn);
    }
  }

  async function loadAvailability() {
    if (!token) return;
    try {
      slotMsgEl.textContent = 'Loading slots...';
      const ignore = manageToken && state.managedBookingId ? `&ignoreBookingId=${encodeURIComponent(state.managedBookingId)}` : '';
      const data = await api(`/booking/${encodeURIComponent(token)}/availability?serviceId=${encodeURIComponent(state.selectedServiceId || '')}&date=${encodeURIComponent(dateEl.value)}&days=14${ignore}`);
      state.dayOnly = data?.dayOnly === true;
      renderDays(Array.isArray(data?.days) ? data.days : []);
      const day = (Array.isArray(data?.days) ? data.days : []).find((d) => String(d?.date || '') === String(dateEl.value)) || data?.days?.[0] || null;
      const daySlots = Array.isArray(day?.slots) ? day.slots : [];
      if (!day && data?.days?.length) dateEl.value = String(data.days[0].date || dateEl.value);
      if (state.dayOnly) {
        state.selectedSlot = daySlots[0] || null;
        slotMsgEl.textContent = daySlots.length
          ? 'This service is multi-day. Pick an open start day.'
          : 'No open start days in this range.';
      } else {
        renderSlots(daySlots);
      }
    } catch (err) {
      renderSlots([]);
      slotMsgEl.textContent = err?.message || 'Failed to load slots.';
    }
  }

  async function init() {
    if (!token) {
      setStatus('Invalid booking link.', true);
      return;
    }
    dateEl.value = toDateInput(Date.now());
    try {
      const cfg = await api(`/booking/${encodeURIComponent(token)}/config`);
      bizNameEl.textContent = String(cfg?.businessName || 'Book Appointment');
      state.timezone = String(cfg?.timezone || 'America/New_York');
      state.services = Array.isArray(cfg?.services) ? cfg.services : [];
      const fromQuery = state.services.find((s) => String(s.id || '') === serviceIdFromQuery) || null;
      const fallback = state.services[0] || null;
      const selected = fromQuery || fallback;
      state.selectedServiceId = String(selected?.id || serviceIdFromQuery || '');
      state.selectedServiceName = String(serviceNameFromQuery || selected?.name || state.selectedServiceId || 'Appointment').trim();
      subLineEl.textContent = `Timezone: ${state.timezone}`;
      renderSummary();
      await loadAvailability();
    } catch (err) {
      setStatus(err?.message || 'Failed to load booking page.', true);
    }
  }

  dateEl.addEventListener('change', () => loadAvailability());

  bookBtn.addEventListener('click', async () => {
    setStatus('');
    if (!nameEl.value.trim()) {
      setStatus('Name is required.', true);
      return;
    }
    if (!phoneEl.value.trim()) {
      setStatus('Phone is required.', true);
      return;
    }
    if (!emailEl.value.trim()) {
      setStatus('Email is required so we can send the invoice.', true);
      return;
    }
    if (!/\S+@\S+\.\S+/.test(emailEl.value.trim())) {
      setStatus('Enter a valid email address.', true);
      return;
    }
    if (!state.selectedSlot) {
      setStatus('Please choose a time slot.', true);
      return;
    }
    const svc = state.services.find((x) => String(x.id) === String(state.selectedServiceId)) || {};
    const selectedServiceName = String(state.selectedServiceName || svc?.name || state.selectedServiceId || 'Appointment').trim();
    bookBtn.disabled = true;
    try {
      const res = await api(`/booking/${encodeURIComponent(token)}/book`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerName: nameEl.value.trim(),
          customerPhone: phoneEl.value.trim(),
          customerEmail: emailEl.value.trim(),
          serviceId: state.selectedServiceId,
          serviceName: selectedServiceName,
          start: Number(state.selectedSlot.start),
          end: Number(state.selectedSlot.end)
        })
      });
      setStatus('Booked successfully.');
      const invoice = res?.invoice || null;
      const paymentUrl = String(invoice?.payment?.url || '').trim();
      renderPostBookingLinks({ manageUrl: res?.manageUrl || '', paymentUrl });
      if (res?.booking?.start) {
        const when = new Date(Number(res.booking.start)).toLocaleString([], { timeZone: state.timezone });
        slotMsgEl.textContent = `Confirmed: ${when}`;
      }
      const pdfUrl = String(invoice?.pdf?.url || '').trim();
      const emailDelivered = Number(invoice?.email?.delivered || 0) > 0;
      const emailErr = String(invoice?.email?.firstError || '').trim();
      if (paymentUrl && pdfUrl && emailDelivered) {
        setStatus(`Booked successfully. Invoice sent to ${emailEl.value.trim()} with a secure payment link.`);
      } else if (paymentUrl && pdfUrl) {
        setStatus('Booked successfully. Use the Pay invoice link to pay securely by card.');
      } else if (pdfUrl && emailDelivered) {
        setStatus(`Booked successfully. Invoice sent to ${emailEl.value.trim()} and PDF generated.`);
      } else if (pdfUrl && !emailDelivered) {
        setStatus(`Booked successfully. Invoice PDF generated, but email delivery failed${emailErr ? `: ${emailErr}` : '.'}`, true);
      } else {
        setStatus('Booked successfully, but invoice generation is pending.', true);
      }
    } catch (err) {
      setStatus(err?.message || 'Failed to book slot.', true);
      await loadAvailability();
    } finally {
      bookBtn.disabled = false;
    }
  });

  async function loadManageContext() {
    if (!manageToken) return;
    try {
      const data = await api(`/booking/${encodeURIComponent(token)}/manage/${encodeURIComponent(manageToken)}`);
      const b = data?.booking || {};
      state.managedBookingId = String(b.id || '');
      if (String(b.status || '').toLowerCase() === 'canceled') {
        setStatus('This booking is canceled.', true);
      }
      manageMsgEl.classList.remove('hidden');
      manageMsgEl.textContent = `Managing booking ${state.managedBookingId}`;
      nameEl.value = String(b.customerName || '');
      phoneEl.value = String(b.customerPhone || '');
      emailEl.value = String(b.customerEmail || '');
      if (b.serviceId) {
        state.selectedServiceId = String(b.serviceId);
      }
      state.selectedServiceName = String(b.serviceName || serviceNameFromQuery || state.selectedServiceName || 'Appointment').trim();
      renderSummary();
      if (Number.isFinite(Number(b.start))) {
        dateEl.value = toDateInput(Number(b.start));
      }
      bookBtn.classList.add('hidden');
      rescheduleBtn.classList.remove('hidden');
      cancelBtn.classList.remove('hidden');
      await loadAvailability();
      if (state.dayOnly && state.availabilityDays.length) {
        const day = state.availabilityDays.find((d) => Array.isArray(d?.slots) && d.slots.length > 0);
        if (day) dateEl.value = String(day.date || dateEl.value);
      }
    } catch (err) {
      setStatus(err?.message || 'Invalid manage link.', true);
    }
  }

  rescheduleBtn.addEventListener('click', async () => {
    setStatus('');
    if (!manageToken) return;
    if (!state.selectedSlot) {
      setStatus('Choose a new time slot first.', true);
      return;
    }
    rescheduleBtn.disabled = true;
    try {
      const res = await api(`/booking/${encodeURIComponent(token)}/manage/${encodeURIComponent(manageToken)}/reschedule`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start: Number(state.selectedSlot.start),
          end: Number(state.selectedSlot.end)
        })
      });
      const when = new Date(Number(res?.booking?.start || 0)).toLocaleString([], { timeZone: state.timezone });
      setStatus(`Rescheduled to ${when}.`);
      await loadAvailability();
    } catch (err) {
      setStatus(err?.message || 'Failed to reschedule.', true);
      await loadAvailability();
    } finally {
      rescheduleBtn.disabled = false;
    }
  });

  cancelBtn.addEventListener('click', async () => {
    setStatus('');
    if (!manageToken) return;
    cancelBtn.disabled = true;
    try {
      await api(`/booking/${encodeURIComponent(token)}/manage/${encodeURIComponent(manageToken)}/cancel`, {
        method: 'POST'
      });
      setStatus('Booking canceled.');
      await loadAvailability();
    } catch (err) {
      setStatus(err?.message || 'Failed to cancel booking.', true);
    } finally {
      cancelBtn.disabled = false;
    }
  });

  init().then(loadManageContext);
})();
