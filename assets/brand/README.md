# OrgPulse Brand Assets

- `orgpulse-icon.svg`: Primary square application icon with the full brand mark.
- `favicon.svg`: Reduced-detail favicon source optimized for browser tabs and small UI surfaces.
- `favicon.ico`, `favicon-16x16.png`, `favicon-32x32.png`: Ready-to-ship browser favicon outputs.
- `apple-touch-icon.png`: Opaque iOS home screen icon output.
- `android-chrome-192x192.png`, `android-chrome-512x512.png`: Android and PWA icon outputs.

These assets intentionally focus on icon surfaces. A future website wordmark can pair the icon with CSS or outlined lettering without baking runtime font dependencies into the canonical asset set.

For future website integration:

```html
<link rel="icon" href="/assets/brand/favicon.svg" type="image/svg+xml" />
<link rel="icon" href="/assets/brand/favicon-32x32.png" sizes="32x32" />
<link rel="icon" href="/assets/brand/favicon.ico" sizes="any" />
<link rel="apple-touch-icon" href="/assets/brand/apple-touch-icon.png" />
```
