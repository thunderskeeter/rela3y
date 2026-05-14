(function (global) {
  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function renderEmptyState(options = {}) {
    const title = String(options.title || "").trim();
    const text = String(options.text || "").trim();
    const className = String(options.className || "").trim();
    const actionsHtml = String(options.actionsHtml || "").trim();
    const centered = options.centered === true;
    const classes = ["empty-state"];
    if (centered) classes.push("is-centered");
    if (className) classes.push(className);
    return `
      <div class="${classes.join(" ")}">
        ${title ? `<div class="h1" style="margin:0;">${escapeHtml(title)}</div>` : ""}
        ${text ? `<p>${escapeHtml(text)}</p>` : ""}
        ${actionsHtml ? `<div class="empty-state-actions">${actionsHtml}</div>` : ""}
      </div>
    `;
  }

  function renderNoticeCard(options = {}) {
    const title = String(options.title || "").trim();
    const text = String(options.text || "").trim();
    const detail = String(options.detail || "").trim();
    const className = String(options.className || "").trim();
    const classes = ["card", "shell-notice"];
    if (className) classes.push(className);
    return `
      <div class="${classes.join(" ")}">
        ${title ? `<p class="h1">${escapeHtml(title)}</p>` : ""}
        ${text ? `<p class="p">${escapeHtml(text)}</p>` : ""}
        ${detail ? `<pre>${escapeHtml(detail)}</pre>` : ""}
      </div>
    `;
  }

  function renderSegmentedControl(options = {}) {
    const activeValue = String(options.activeValue ?? "").trim();
    const dataAttr = String(options.dataAttr || "").trim();
    const className = String(options.className || "").trim();
    const controls = Array.isArray(options.options) ? options.options : [];
    const classes = ["ui-segmented-control"];
    if (className) classes.push(className);
    const attrName = dataAttr ? ` data-${escapeHtml(dataAttr)}` : "";
    return `
      <div class="${classes.join(" ")}">
        ${controls.map((option) => {
          const value = String(option?.value ?? "").trim();
          const label = String(option?.label ?? value).trim();
          const active = value === activeValue ? " active" : "";
          return `<button class="btn${active}"${attrName}="${escapeHtml(value)}">${escapeHtml(label)}</button>`;
        }).join("")}
      </div>
    `;
  }

  function debounce(fn, wait) {
    var timeoutId = 0;
    var delay = Math.max(0, Number(wait || 0));
    return function debounced() {
      var ctx = this;
      var args = arguments;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
      timeoutId = setTimeout(function () {
        timeoutId = 0;
        fn.apply(ctx, args);
      }, delay);
    };
  }

  global.RelayUI = {
    escapeHtml,
    renderEmptyState,
    renderNoticeCard,
    renderSegmentedControl,
    debounce,
  };
})(window);
