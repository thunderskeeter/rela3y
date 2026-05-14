(function () {
  try {
    var key = 'arc_relay_landing_theme';
    var saved = localStorage.getItem(key);
    document.documentElement.setAttribute('data-theme', saved === 'light' ? 'light' : 'dark');
  } catch (_) {
    document.documentElement.setAttribute('data-theme', 'dark');
  }

  document.addEventListener('DOMContentLoaded', function () {
    var key = 'arc_relay_landing_theme';
    var root = document.documentElement;
    var themeBtn = document.getElementById('themeToggle');
    var pricingOverlay = document.getElementById('pricingOverlay');
    var pricingCloseBtn = document.getElementById('pricingCloseBtn');
    var pricingOpenButtons = Array.prototype.slice.call(document.querySelectorAll('[data-open-pricing="1"]'));
    var checkoutButtons = Array.prototype.slice.call(document.querySelectorAll('[data-checkout-plan]'));
    var checkoutBusinessName = document.getElementById('checkoutBusinessName');
    var checkoutEmail = document.getElementById('checkoutEmail');
    var checkoutStatus = document.getElementById('checkoutStatus');
    var reveals = Array.prototype.slice.call(document.querySelectorAll('[data-reveal]'));

    function updateThemeLabel() {
      if (!themeBtn) return;
      var mode = root.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
      themeBtn.setAttribute('aria-label', mode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode');
      themeBtn.setAttribute('title', mode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode');
    }

    function openPricing() {
      if (!pricingOverlay) return;
      pricingOverlay.classList.remove('hidden');
      pricingOverlay.setAttribute('aria-hidden', 'false');
    }

    function closePricing() {
      if (!pricingOverlay) return;
      pricingOverlay.classList.add('hidden');
      pricingOverlay.setAttribute('aria-hidden', 'true');
    }

    async function startCheckout(planKey) {
      var email = String((checkoutEmail && checkoutEmail.value) || '').trim();
      var businessName = String((checkoutBusinessName && checkoutBusinessName.value) || '').trim();
      if (!businessName || !email) {
        if (checkoutStatus) checkoutStatus.textContent = 'Enter business name and work email to continue.';
        return;
      }
      if (checkoutStatus) checkoutStatus.textContent = 'Redirecting to secure checkout...';
      try {
        var successUrl = window.location.origin + '/dashboard?newClient=1&checkout=success';
        var cancelUrl = window.location.origin + '/home?checkout=cancel';
        var response = await fetch('/api/public/onboarding/checkout-session', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ planKey: planKey, email: email, businessName: businessName, successUrl: successUrl, cancelUrl: cancelUrl })
        });
        var payload = await response.json().catch(function () { return {}; });
        if (!response.ok) throw new Error(String((payload && payload.error) || ('Checkout failed (' + response.status + ')')));
        var url = String((payload && payload.url) || '').trim();
        if (!url) throw new Error('Checkout URL missing.');
        window.location.href = url;
      } catch (error) {
        if (checkoutStatus) checkoutStatus.textContent = String((error && error.message) || 'Unable to start checkout.');
      }
    }

    updateThemeLabel();

    if (themeBtn) {
      themeBtn.addEventListener('click', function () {
        var current = root.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
        var next = current === 'dark' ? 'light' : 'dark';
        root.setAttribute('data-theme', next);
        try { localStorage.setItem(key, next); } catch (_) {}
        updateThemeLabel();
      });
    }

    pricingOpenButtons.forEach(function (button) {
      button.addEventListener('click', function (event) {
        event.preventDefault();
        openPricing();
      });
    });
    if (pricingCloseBtn) pricingCloseBtn.addEventListener('click', closePricing);
    if (pricingOverlay) {
      pricingOverlay.addEventListener('click', function (event) {
        if (event.target === pricingOverlay) closePricing();
      });
    }
    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape') closePricing();
    });

    checkoutButtons.forEach(function (button) {
      button.addEventListener('click', function () {
        var planKey = String(button.getAttribute('data-checkout-plan') || '').trim().toLowerCase();
        if (!planKey) return;
        void startCheckout(planKey);
      });
    });

    if ('IntersectionObserver' in window) {
      var observer = new IntersectionObserver(function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) {
            entry.target.classList.add('in');
            observer.unobserve(entry.target);
          }
        });
      }, { threshold: 0.12, rootMargin: '0px 0px -8% 0px' });
      reveals.forEach(function (element) { observer.observe(element); });
    } else {
      reveals.forEach(function (element) { element.classList.add('in'); });
    }
  });
})();
