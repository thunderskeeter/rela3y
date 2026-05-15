# Mobile QA Checklist

## Viewports

- [x] 360x800 small Android
- [x] 390x844 iPhone 13/14
- [x] 430x932 large iPhone
- [x] 768x1024 tablet
- [x] 1024x768 tablet landscape
- [x] 1440x900 desktop

## Fixed Coverage

- [x] Global shell prevents accidental page-level horizontal scrolling.
- [x] Bottom navigation dock compresses to a six-column mobile grid on narrow phones.
- [x] Topbar icon controls keep 44px minimum tap targets on mobile.
- [x] Shared buttons, inputs, selects, and textareas respect mobile tap/input sizing.
- [x] Settings, billing, team, command, and other modals fit within the viewport and scroll internally.
- [x] Form rows and action bars wrap/stack cleanly on phone screens.
- [x] Tables use intentional horizontal scroll containers instead of overflowing the page.
- [x] Platform users and phone-number tables are included in table overflow handling.
- [x] Dashboard KPI/cards stack at phone widths while keeping desktop grids intact.
- [x] Messages view stacks the thread list and chat panel for mobile use.
- [x] Schedule/calendar views release fixed-height overflow on mobile and keep calendars scrollable.
- [x] Contacts, analytics, billing, and developer settings headers/actions wrap without clipping.
