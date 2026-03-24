const themes = {
    default: {},

    cyber: {
        '--font-family': "'Courier New', 'Lucida Console', monospace",

        '--color-bg': '#0a0a0a',
        '--color-surface': '#0d1117',
        '--color-surface-raised': '#0a0e14',
        '--color-surface-hover': '#111e2a',
        '--color-surface-active': '#0f1a24',

        '--color-border': '#0f3',
        '--color-border-subtle': '#0a1a10',
        '--color-border-hover': '#0f6',
        '--color-border-card': '#0b2e14',

        '--color-text': '#0f0',
        '--color-text-muted': '#0a7a30',
        '--color-text-secondary': '#0c9940',
        '--color-text-dim': '#0bb848',
        '--color-text-bright': '#3f3',
        '--color-text-placeholder': '#064a1c',

        '--color-selected': '#0a2e14',

        '--color-primary': '#00cc44',
        '--color-primary-hover': '#00ff55',
        '--color-btn-active': '#00aa33',

        '--color-offline-bg': '#1a0a0a',
        '--color-offline-text': '#ff2040',

        '--color-stale-bg': '#0a1a0a',
        '--color-stale-text': '#66cc66',
        '--opacity-stale': '0.5',

        '--color-dup-bg': '#1a1a00',
        '--color-dup-text': '#ff0',

        '--color-tag-bg': '#001a1a',
        '--color-tag-text': '#0ff',

        '--color-scan-bg': '#001a00',
        '--color-scan-border': '#003a00',
        '--color-scan-text': '#0f0',
        '--color-progress-track': '#0a1a0a',
        '--color-progress-fill': '#0f0',

        '--color-overlay': 'rgba(0, 10, 0, 0.8)',

        '--color-statusbar-bg': '#050a05',
        '--color-status-ok': '#0f0',
        '--color-status-warn': '#ff0',
        '--color-status-error': '#f00',

        '--text-glow': '0 0 4px currentColor',
        '--scanline-opacity': '1',
        '--title-letter-spacing': '0.2em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 8px currentColor, 0 0 20px currentColor',
        '--glow-primary': '0 0 6px var(--color-primary), inset 0 0 6px rgba(0, 204, 68, 0.15)',
        '--glow-primary-hover': '0 0 12px var(--color-primary-hover), inset 0 0 10px rgba(0, 255, 85, 0.2)',
        '--glow-focus': '0 0 6px var(--color-primary)',
        '--glow-modal': '0 0 30px rgba(0, 255, 0, 0.15), 0 0 60px rgba(0, 255, 0, 0.05)',
        '--glow-stat': '0 0 8px rgba(0, 255, 0, 0.08)',
        '--glow-status-dot': '0 0 6px var(--color-status-ok), 0 0 12px var(--color-status-ok)',
        '--glow-dup': '0 0 6px var(--color-dup-text)',
        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(0, 255, 85, 0.1)',
        '--btn-primary-text': 'var(--color-primary)',

        '--treemap-color-1': '#00aa00',
        '--treemap-color-2': '#008800',
        '--treemap-color-3': '#00cc44',
        '--treemap-color-4': '#22aa22',
        '--treemap-color-5': '#00bb66',
        '--treemap-color-6': '#44aa00',
        '--treemap-color-7': '#009944',
        '--treemap-color-8': '#66bb00',
    },

    c64: {
        // Commodore 64 (1982) — based on the MOS 6567/6569 VIC-II 16-colour palette
        // Boot screen: light blue (colour 14) text on blue (colour 6) background
        '--font-family': "'Courier New', 'Lucida Console', monospace",

        // Surfaces — built around C64 blue (colour 6: #352879)
        '--color-bg': '#352879',
        '--color-surface': '#3d308a',
        '--color-surface-raised': '#453898',
        '--color-surface-hover': '#5043a6',
        '--color-surface-active': '#4a3d9e',

        // Borders — light blue (colour 14: #6c5eb5)
        '--color-border': '#6c5eb5',
        '--color-border-subtle': '#453898',
        '--color-border-hover': '#9a8ee0',
        '--color-border-card': '#584aaa',

        // Text — light blue tones
        '--color-text': '#a09aee',
        '--color-text-muted': '#6c5eb5',
        '--color-text-secondary': '#867acc',
        '--color-text-dim': '#9690dd',
        '--color-text-bright': '#ffffff',
        '--color-text-placeholder': '#584aaa',

        '--color-selected': '#5043b0',

        // Primary — C64 cyan (colour 3: #70a4b2)
        '--color-primary': '#70a4b2',
        '--color-primary-hover': '#8cc0cc',
        '--color-btn-active': '#5e909e',

        // Offline — C64 red (colour 2: #68372b) / light red (colour 10: #9a6759)
        '--color-offline-bg': '#4a2828',
        '--color-offline-text': '#9a6759',

        // Stale — C64 purple (colour 4: #6f3d86)
        '--color-stale-bg': '#3a2848',
        '--color-stale-text': '#8e6aaa',
        '--opacity-stale': '0.55',

        // Duplicates — C64 yellow (colour 7: #b8c76f)
        '--color-dup-bg': '#3a3828',
        '--color-dup-text': '#b8c76f',

        // Tags — C64 cyan
        '--color-tag-bg': '#2a3848',
        '--color-tag-text': '#70a4b2',

        // Scan — C64 green (colour 5: #588d43) / light green (colour 13: #9ad284)
        '--color-scan-bg': '#2e3828',
        '--color-scan-border': '#406838',
        '--color-scan-text': '#9ad284',
        '--color-progress-track': '#3d308a',
        '--color-progress-fill': '#70a4b2',

        '--color-overlay': 'rgba(53, 40, 121, 0.85)',

        // Status bar — deeper blue
        '--color-statusbar-bg': '#2a2068',
        '--color-status-ok': '#9ad284',
        '--color-status-warn': '#b8c76f',
        '--color-status-error': '#9a6759',

        // Effects — subtle CRT phosphor glow, authentic scanlines
        '--text-glow': '0 0 3px rgba(160, 154, 238, 0.4)',
        '--scanline-opacity': '0.5',
        '--title-letter-spacing': '0.15em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 6px rgba(160, 154, 238, 0.5)',
        '--glow-primary': 'none',
        '--glow-primary-hover': 'none',
        '--glow-focus': '0 0 4px rgba(112, 164, 178, 0.5)',
        '--glow-modal': '0 0 20px rgba(108, 94, 181, 0.25)',
        '--glow-stat': 'none',
        '--glow-status-dot': '0 0 4px var(--color-status-ok)',
        '--glow-dup': 'none',
        '--btn-primary-bg': 'var(--color-primary)',
        '--btn-primary-bg-hover': 'var(--color-primary-hover)',
        '--btn-primary-text': '#352879',

        '--treemap-color-1': '#6c5eb5',
        '--treemap-color-2': '#70a4b2',
        '--treemap-color-3': '#9ad284',
        '--treemap-color-4': '#b8c76f',
        '--treemap-color-5': '#9a6759',
        '--treemap-color-6': '#7c70da',
        '--treemap-color-7': '#588d43',
        '--treemap-color-8': '#a09aee',
    },

    neon: {
        // Neon — hot pink / electric blue on near-black, modern synthwave vibe
        '--font-family': "'Segoe UI', Roboto, 'Helvetica Neue', sans-serif",

        '--color-bg': '#0c0c14',
        '--color-surface': '#12121e',
        '--color-surface-raised': '#161626',
        '--color-surface-hover': '#1e1e32',
        '--color-surface-active': '#1a1a2c',

        '--color-border': '#2a2a44',
        '--color-border-subtle': '#1a1a2e',
        '--color-border-hover': '#ff2d95',
        '--color-border-card': '#22223a',

        '--color-text': '#e0dff0',
        '--color-text-muted': '#6868a0',
        '--color-text-secondary': '#9090c0',
        '--color-text-dim': '#b0b0d0',
        '--color-text-bright': '#ffffff',
        '--color-text-placeholder': '#404068',

        '--color-selected': '#2a1838',

        // Primary — hot pink
        '--color-primary': '#ff2d95',
        '--color-primary-hover': '#ff5cb0',
        '--color-btn-active': '#d4207a',

        '--color-offline-bg': '#1e0c0c',
        '--color-offline-text': '#ff4060',

        '--color-stale-bg': '#1a0c20',
        '--color-stale-text': '#c060ff',
        '--opacity-stale': '0.55',

        // Duplicates — electric yellow
        '--color-dup-bg': '#1e1e08',
        '--color-dup-text': '#ffe040',

        // Tags — electric blue
        '--color-tag-bg': '#0c1428',
        '--color-tag-text': '#00c8ff',

        '--color-scan-bg': '#0c1410',
        '--color-scan-border': '#18302a',
        '--color-scan-text': '#00e8a0',
        '--color-progress-track': '#161626',
        '--color-progress-fill': '#ff2d95',

        '--color-overlay': 'rgba(6, 6, 12, 0.85)',

        '--color-statusbar-bg': '#08080f',
        '--color-status-ok': '#00e8a0',
        '--color-status-warn': '#ffe040',
        '--color-status-error': '#ff4060',

        // Effects — vivid glow, no scanlines (this is modern, not retro)
        '--text-glow': 'none',
        '--scanline-opacity': '0',
        '--title-letter-spacing': '0.08em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 10px #ff2d95, 0 0 30px rgba(255, 45, 149, 0.4)',
        '--glow-primary': '0 0 8px rgba(255, 45, 149, 0.4), inset 0 0 8px rgba(255, 45, 149, 0.1)',
        '--glow-primary-hover': '0 0 16px rgba(255, 92, 176, 0.5), inset 0 0 12px rgba(255, 92, 176, 0.15)',
        '--glow-focus': '0 0 8px rgba(255, 45, 149, 0.4)',
        '--glow-modal': '0 0 40px rgba(255, 45, 149, 0.15), 0 0 80px rgba(0, 200, 255, 0.05)',
        '--glow-stat': '0 0 10px rgba(0, 200, 255, 0.08)',
        '--glow-status-dot': '0 0 6px var(--color-status-ok), 0 0 14px var(--color-status-ok)',
        '--glow-dup': '0 0 6px rgba(255, 224, 64, 0.5)',
        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(255, 45, 149, 0.1)',
        '--btn-primary-text': '#ff2d95',

        '--treemap-color-1': '#ff2d95',
        '--treemap-color-2': '#00c8ff',
        '--treemap-color-3': '#00e8a0',
        '--treemap-color-4': '#ffe040',
        '--treemap-color-5': '#9040ff',
        '--treemap-color-6': '#ff6030',
        '--treemap-color-7': '#40e0d0',
        '--treemap-color-8': '#ff60c0',
    },

    light: {
        // Light — warm, readable, editorial feel
        '--font-family': "Georgia, 'Times New Roman', serif",

        '--color-bg': '#f0efe8',
        '--color-surface': '#ffffff',
        '--color-surface-raised': '#f8f7f2',
        '--color-surface-hover': '#eae9e2',
        '--color-surface-active': '#e4e3dc',

        '--color-border': '#cccbc4',
        '--color-border-subtle': '#e4e3dc',
        '--color-border-hover': '#2563eb',
        '--color-border-card': '#d8d7d0',

        '--color-text': '#2a2a28',
        '--color-text-muted': '#787870',
        '--color-text-secondary': '#585850',
        '--color-text-dim': '#484840',
        '--color-text-bright': '#000000',
        '--color-text-placeholder': '#a8a8a0',

        '--color-selected': '#dbeafe',

        '--color-primary': '#2563eb',
        '--color-primary-hover': '#1d4fd8',
        '--color-btn-active': '#1e40af',

        '--color-offline-bg': '#fef2f2',
        '--color-offline-text': '#dc2626',

        '--color-stale-bg': '#f5f0ff',
        '--color-stale-text': '#7c3aed',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#fefce8',
        '--color-dup-text': '#a16207',

        '--color-tag-bg': '#eff6ff',
        '--color-tag-text': '#2563eb',

        '--color-scan-bg': '#f0fdf4',
        '--color-scan-border': '#bbf7d0',
        '--color-scan-text': '#16a34a',
        '--color-progress-track': '#e4e3dc',
        '--color-progress-fill': '#2563eb',

        '--color-overlay': 'rgba(0, 0, 0, 0.35)',

        '--color-statusbar-bg': '#f8f7f2',
        '--color-status-ok': '#16a34a',
        '--color-status-warn': '#d97706',
        '--color-status-error': '#dc2626',

        '--btn-primary-bg': 'var(--color-primary)',
        '--btn-primary-bg-hover': 'var(--color-primary-hover)',
        '--btn-primary-text': '#ffffff',

        '--treemap-color-1': '#4a7aba',
        '--treemap-color-2': '#2a8a7a',
        '--treemap-color-3': '#7a5ac0',
        '--treemap-color-4': '#c06030',
        '--treemap-color-5': '#4a8a3a',
        '--treemap-color-6': '#c04040',
        '--treemap-color-7': '#a07820',
        '--treemap-color-8': '#9050a0',
    },

    black: {
        // Black — pure OLED black, monochrome, brutally minimal
        '--font-family': "'Helvetica Neue', Helvetica, Arial, sans-serif",

        '--color-bg': '#000000',
        '--color-surface': '#0a0a0a',
        '--color-surface-raised': '#111111',
        '--color-surface-hover': '#1a1a1a',
        '--color-surface-active': '#151515',

        '--color-border': '#222222',
        '--color-border-subtle': '#141414',
        '--color-border-hover': '#ffffff',
        '--color-border-card': '#1c1c1c',

        '--color-text': '#a0a0a0',
        '--color-text-muted': '#555555',
        '--color-text-secondary': '#707070',
        '--color-text-dim': '#888888',
        '--color-text-bright': '#ffffff',
        '--color-text-placeholder': '#333333',

        '--color-selected': '#1a1a1a',

        '--color-primary': '#ffffff',
        '--color-primary-hover': '#cccccc',
        '--color-btn-active': '#aaaaaa',

        '--color-offline-bg': '#140808',
        '--color-offline-text': '#a04040',

        '--color-stale-bg': '#100810',
        '--color-stale-text': '#806080',
        '--opacity-stale': '0.5',

        '--color-dup-bg': '#141408',
        '--color-dup-text': '#a0a040',

        '--color-tag-bg': '#080814',
        '--color-tag-text': '#6868a0',

        '--color-scan-bg': '#081008',
        '--color-scan-border': '#183018',
        '--color-scan-text': '#40a040',
        '--color-progress-track': '#111111',
        '--color-progress-fill': '#ffffff',

        '--color-overlay': 'rgba(0, 0, 0, 0.9)',

        '--color-statusbar-bg': '#050505',
        '--color-status-ok': '#40a040',
        '--color-status-warn': '#a0a040',
        '--color-status-error': '#a04040',

        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(255, 255, 255, 0.06)',
        '--btn-primary-text': '#ffffff',

        '--treemap-color-1': '#707070',
        '--treemap-color-2': '#4a4a4a',
        '--treemap-color-3': '#888888',
        '--treemap-color-4': '#3a3a3a',
        '--treemap-color-5': '#606060',
        '--treemap-color-6': '#555555',
        '--treemap-color-7': '#7a7a7a',
        '--treemap-color-8': '#484848',
    },

    red: {
        // Red — dark crimson, menacing, sharp
        '--font-family': "'Trebuchet MS', 'Lucida Sans', sans-serif",

        '--color-bg': '#100404',
        '--color-surface': '#180808',
        '--color-surface-raised': '#1e0c0c',
        '--color-surface-hover': '#2a1212',
        '--color-surface-active': '#241010',

        '--color-border': '#3a1818',
        '--color-border-subtle': '#241010',
        '--color-border-hover': '#dd2222',
        '--color-border-card': '#301414',

        '--color-text': '#d4a8a8',
        '--color-text-muted': '#804848',
        '--color-text-secondary': '#a06868',
        '--color-text-dim': '#b88080',
        '--color-text-bright': '#ffe0e0',
        '--color-text-placeholder': '#5a2828',

        '--color-selected': '#2a0e0e',

        '--color-primary': '#dd2222',
        '--color-primary-hover': '#ee3838',
        '--color-btn-active': '#bb1a1a',

        '--color-offline-bg': '#1e0606',
        '--color-offline-text': '#ff5050',

        '--color-stale-bg': '#1a0c14',
        '--color-stale-text': '#a06080',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#1a1808',
        '--color-dup-text': '#d4a030',

        '--color-tag-bg': '#140810',
        '--color-tag-text': '#c060a0',

        '--color-scan-bg': '#0a1408',
        '--color-scan-border': '#1a3018',
        '--color-scan-text': '#80c060',
        '--color-progress-track': '#1e0c0c',
        '--color-progress-fill': '#dd2222',

        '--color-overlay': 'rgba(10, 2, 2, 0.85)',

        '--color-statusbar-bg': '#0c0404',
        '--color-status-ok': '#80c060',
        '--color-status-warn': '#d4a030',
        '--color-status-error': '#ff5050',

        '--text-glow': '0 0 3px rgba(220, 34, 34, 0.2)',
        '--title-letter-spacing': '0.06em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 8px rgba(220, 34, 34, 0.5), 0 0 20px rgba(220, 34, 34, 0.15)',
        '--glow-primary': '0 0 6px rgba(220, 34, 34, 0.3), inset 0 0 6px rgba(220, 34, 34, 0.1)',
        '--glow-primary-hover': '0 0 12px rgba(238, 56, 56, 0.4), inset 0 0 10px rgba(238, 56, 56, 0.15)',
        '--glow-focus': '0 0 6px rgba(220, 34, 34, 0.3)',
        '--glow-modal': '0 0 30px rgba(220, 34, 34, 0.12)',
        '--glow-status-dot': '0 0 4px var(--color-status-ok)',
        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(220, 34, 34, 0.1)',
        '--btn-primary-text': '#dd2222',

        '--treemap-color-1': '#cc3333',
        '--treemap-color-2': '#aa2020',
        '--treemap-color-3': '#dd5544',
        '--treemap-color-4': '#993333',
        '--treemap-color-5': '#cc5522',
        '--treemap-color-6': '#bb4444',
        '--treemap-color-7': '#aa3322',
        '--treemap-color-8': '#dd6655',
    },

    girly: {
        // Girly — pastel pink, playful, bubbly
        '--font-family': "'Comic Sans MS', 'Chalkboard SE', cursive",

        '--color-bg': '#fff0f6',
        '--color-surface': '#fff8fb',
        '--color-surface-raised': '#ffe8f2',
        '--color-surface-hover': '#ffd8ea',
        '--color-surface-active': '#ffe0ee',

        '--color-border': '#f0b8d0',
        '--color-border-subtle': '#fce0ee',
        '--color-border-hover': '#e84090',
        '--color-border-card': '#f0c8d8',

        '--color-text': '#5a2848',
        '--color-text-muted': '#a06888',
        '--color-text-secondary': '#804868',
        '--color-text-dim': '#6a3858',
        '--color-text-bright': '#3a0828',
        '--color-text-placeholder': '#d0a0b8',

        '--color-selected': '#fce0f0',

        '--color-primary': '#e84090',
        '--color-primary-hover': '#d03080',
        '--color-btn-active': '#c02870',

        '--color-offline-bg': '#fff0f0',
        '--color-offline-text': '#e04848',

        '--color-stale-bg': '#f0e8f8',
        '--color-stale-text': '#9060b0',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#fef8e0',
        '--color-dup-text': '#c07820',

        '--color-tag-bg': '#f0e8ff',
        '--color-tag-text': '#8040c0',

        '--color-scan-bg': '#f0fff0',
        '--color-scan-border': '#c0e8c0',
        '--color-scan-text': '#40a840',
        '--color-progress-track': '#ffe8f2',
        '--color-progress-fill': '#e84090',

        '--color-overlay': 'rgba(90, 40, 72, 0.35)',

        '--color-statusbar-bg': '#fff0f6',
        '--color-status-ok': '#40a840',
        '--color-status-warn': '#d09020',
        '--color-status-error': '#e04848',

        '--btn-primary-bg': 'var(--color-primary)',
        '--btn-primary-bg-hover': 'var(--color-primary-hover)',
        '--btn-primary-text': '#ffffff',

        '--treemap-color-1': '#e84090',
        '--treemap-color-2': '#c060c0',
        '--treemap-color-3': '#80a0e0',
        '--treemap-color-4': '#e08060',
        '--treemap-color-5': '#60c080',
        '--treemap-color-6': '#d0a040',
        '--treemap-color-7': '#a070d0',
        '--treemap-color-8': '#e06080',
    },

    'amber monitor': {
        // Amber Monitor — 1980s amber phosphor CRT terminal
        '--font-family': "'Lucida Console', Monaco, monospace",

        '--color-bg': '#0a0800',
        '--color-surface': '#100e04',
        '--color-surface-raised': '#161208',
        '--color-surface-hover': '#201a0c',
        '--color-surface-active': '#1c160a',

        '--color-border': '#3a2e00',
        '--color-border-subtle': '#1c160a',
        '--color-border-hover': '#ffb800',
        '--color-border-card': '#2a2200',

        '--color-text': '#ffb000',
        '--color-text-muted': '#806000',
        '--color-text-secondary': '#c08800',
        '--color-text-dim': '#d89800',
        '--color-text-bright': '#ffd060',
        '--color-text-placeholder': '#504000',

        '--color-selected': '#201a00',

        '--color-primary': '#ffb000',
        '--color-primary-hover': '#ffc830',
        '--color-btn-active': '#d09000',

        '--color-offline-bg': '#1a0808',
        '--color-offline-text': '#cc4020',

        '--color-stale-bg': '#141008',
        '--color-stale-text': '#907040',
        '--opacity-stale': '0.5',

        '--color-dup-bg': '#1a1a00',
        '--color-dup-text': '#ffd040',

        '--color-tag-bg': '#0a1218',
        '--color-tag-text': '#60a0b0',

        '--color-scan-bg': '#0a1008',
        '--color-scan-border': '#1a3010',
        '--color-scan-text': '#80c040',
        '--color-progress-track': '#161208',
        '--color-progress-fill': '#ffb000',

        '--color-overlay': 'rgba(10, 8, 0, 0.85)',

        '--color-statusbar-bg': '#080600',
        '--color-status-ok': '#80c040',
        '--color-status-warn': '#ffd040',
        '--color-status-error': '#cc4020',

        // CRT effects — warm amber phosphor glow
        '--text-glow': '0 0 4px rgba(255, 176, 0, 0.35)',
        '--scanline-opacity': '0.6',
        '--title-letter-spacing': '0.15em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 8px rgba(255, 176, 0, 0.5), 0 0 20px rgba(255, 176, 0, 0.15)',
        '--glow-primary': '0 0 6px rgba(255, 176, 0, 0.25), inset 0 0 6px rgba(255, 176, 0, 0.08)',
        '--glow-primary-hover': '0 0 12px rgba(255, 200, 48, 0.35), inset 0 0 10px rgba(255, 200, 48, 0.12)',
        '--glow-focus': '0 0 5px rgba(255, 176, 0, 0.35)',
        '--glow-modal': '0 0 20px rgba(255, 176, 0, 0.12)',
        '--glow-status-dot': '0 0 4px var(--color-status-ok)',
        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(255, 176, 0, 0.08)',
        '--btn-primary-text': '#ffb000',

        '--treemap-color-1': '#cc8800',
        '--treemap-color-2': '#aa6600',
        '--treemap-color-3': '#ddaa22',
        '--treemap-color-4': '#bb7700',
        '--treemap-color-5': '#ccaa44',
        '--treemap-color-6': '#aa8822',
        '--treemap-color-7': '#dd9900',
        '--treemap-color-8': '#998844',
    },

    solaris: {
        // Solaris — solar observatory: the cold void of deep space illuminated
        // by the fierce golden light of a star. Inspired by false-colour solar
        // imagery and the visual drama of a total eclipse — darkness meets
        // brilliance. Pale starlight text, warm solar-gold accents, and a
        // three-layer corona glow on the title.
        '--font-family': "'Avenir Next', 'Avenir', 'Century Gothic', 'Futura', -apple-system, sans-serif",

        // Surfaces — deep space indigo, barely distinguishable layers
        '--color-bg': '#0c0a16',
        '--color-surface': '#12101e',
        '--color-surface-raised': '#181428',
        '--color-surface-hover': '#221e36',
        '--color-surface-active': '#1e1a30',

        // Borders — muted violet, golden hover reveals the star's light
        '--color-border': '#302848',
        '--color-border-subtle': '#1e1a30',
        '--color-border-hover': '#d4943c',
        '--color-border-card': '#282240',

        // Text — starlight: pale lavender tones that contrast against warm gold
        '--color-text': '#c4bcd4',
        '--color-text-muted': '#685e80',
        '--color-text-secondary': '#8e84a8',
        '--color-text-dim': '#a69cc0',
        '--color-text-bright': '#f0e8ff',
        '--color-text-placeholder': '#443c58',

        '--color-selected': '#2a2048',

        // Primary — solar gold, the hero accent
        '--color-primary': '#d4943c',
        '--color-primary-hover': '#e8a850',
        '--color-btn-active': '#c08430',

        // Offline — red dwarf
        '--color-offline-bg': '#1a0a0c',
        '--color-offline-text': '#e05050',

        // Stale — faded nebula
        '--color-stale-bg': '#1a1428',
        '--color-stale-text': '#8878b0',
        '--opacity-stale': '0.55',

        // Duplicates — supernova amber
        '--color-dup-bg': '#1a1808',
        '--color-dup-text': '#e0b840',

        // Tags — cosmic teal (cool contrast to the warm palette)
        '--color-tag-bg': '#0c1820',
        '--color-tag-text': '#50b8c8',

        // Scan — nebula green
        '--color-scan-bg': '#0c180c',
        '--color-scan-border': '#1a3820',
        '--color-scan-text': '#68c860',
        '--color-progress-track': '#181428',
        '--color-progress-fill': '#d4943c',

        '--color-overlay': 'rgba(6, 4, 12, 0.85)',

        // Status bar — deepest void
        '--color-statusbar-bg': '#08060f',
        '--color-status-ok': '#68c860',
        '--color-status-warn': '#e0b840',
        '--color-status-error': '#e05050',

        // Effects — solar corona: warm golden glow, no CRT artifacts
        '--text-glow': 'none',
        '--scanline-opacity': '0',
        '--title-letter-spacing': '0.12em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 10px rgba(212, 148, 60, 0.6), 0 0 30px rgba(212, 148, 60, 0.2), 0 0 60px rgba(212, 148, 60, 0.05)',
        '--glow-primary': '0 0 8px rgba(212, 148, 60, 0.35), inset 0 0 8px rgba(212, 148, 60, 0.08)',
        '--glow-primary-hover': '0 0 14px rgba(232, 168, 80, 0.45), inset 0 0 12px rgba(232, 168, 80, 0.12)',
        '--glow-focus': '0 0 8px rgba(212, 148, 60, 0.4)',
        '--glow-modal': '0 0 40px rgba(212, 148, 60, 0.12), 0 0 80px rgba(212, 148, 60, 0.04)',
        '--glow-stat': '0 0 10px rgba(212, 148, 60, 0.06)',
        '--glow-status-dot': '0 0 6px var(--color-status-ok), 0 0 12px var(--color-status-ok)',
        '--glow-dup': '0 0 6px rgba(224, 184, 64, 0.4)',
        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(212, 148, 60, 0.1)',
        '--btn-primary-text': '#d4943c',

        '--treemap-color-1': '#d4943c',
        '--treemap-color-2': '#68c860',
        '--treemap-color-3': '#50b8c8',
        '--treemap-color-4': '#e05050',
        '--treemap-color-5': '#e0b840',
        '--treemap-color-6': '#8070c0',
        '--treemap-color-7': '#c07048',
        '--treemap-color-8': '#50a0d0',
    },

    'solaris light': {
        // Solaris Light — the same observatory at dawn. The shutters are open,
        // warm sunlight floods the room. Deep indigo-violet text (the void
        // remembered), solar gold accents, pale lavender-cream surfaces
        // catching the light.
        '--font-family': "'Avenir Next', 'Avenir', 'Century Gothic', 'Futura', -apple-system, sans-serif",

        // Surfaces — distinctly lavender, violet saturation prominent
        '--color-bg': '#dfd6f2',
        '--color-surface': '#f2edfc',
        '--color-surface-raised': '#eae2f8',
        '--color-surface-hover': '#d6cced',
        '--color-surface-active': '#d0c6e8',

        // Borders — violet with golden hover
        '--color-border': '#b0a4d0',
        '--color-border-subtle': '#c8bede',
        '--color-border-hover': '#b87820',
        '--color-border-card': '#bab0d8',

        // Text — deep indigo with real weight
        '--color-text': '#1e1834',
        '--color-text-muted': '#706890',
        '--color-text-secondary': '#443c60',
        '--color-text-dim': '#3a3258',
        '--color-text-bright': '#120e24',
        '--color-text-placeholder': '#9890b0',

        // Selection — solar light landing on the surface
        '--color-selected': '#f4e4c8',

        // Primary — rich solar gold
        '--color-primary': '#b07018',
        '--color-primary-hover': '#986010',
        '--color-btn-active': '#885408',

        '--color-offline-bg': '#fceaea',
        '--color-offline-text': '#c02020',

        '--color-stale-bg': '#eee4f4',
        '--color-stale-text': '#6a4c90',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#fcf4dc',
        '--color-dup-text': '#886000',

        '--color-tag-bg': '#dceef4',
        '--color-tag-text': '#1c6878',

        '--color-scan-bg': '#e8f4e8',
        '--color-scan-border': '#a8d4a8',
        '--color-scan-text': '#286828',
        '--color-progress-track': '#c8bede',
        '--color-progress-fill': '#b07018',

        '--color-overlay': 'rgba(26, 20, 48, 0.4)',

        '--color-statusbar-bg': '#dfd6f2',
        '--color-status-ok': '#286828',
        '--color-status-warn': '#886000',
        '--color-status-error': '#c02020',

        // Effects — restrained; corona glow on title only
        '--text-glow': 'none',
        '--scanline-opacity': '0',
        '--title-letter-spacing': '0.12em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 12px rgba(176, 112, 24, 0.3), 0 0 30px rgba(176, 112, 24, 0.1)',
        '--glow-primary': 'none',
        '--glow-primary-hover': 'none',
        '--glow-focus': '0 0 6px rgba(176, 112, 24, 0.3)',
        '--glow-modal': '0 0 30px rgba(26, 20, 48, 0.1)',
        '--glow-stat': 'none',
        '--glow-status-dot': 'none',
        '--glow-dup': 'none',
        '--btn-primary-bg': 'var(--color-primary)',
        '--btn-primary-bg-hover': 'var(--color-primary-hover)',
        '--btn-primary-text': '#ffffff',

        '--treemap-color-1': '#b07018',
        '--treemap-color-2': '#286828',
        '--treemap-color-3': '#1c6878',
        '--treemap-color-4': '#c02020',
        '--treemap-color-5': '#886000',
        '--treemap-color-6': '#5a3c90',
        '--treemap-color-7': '#a05030',
        '--treemap-color-8': '#2060a0',
    },

    'green monitor': {
        // Green Monitor — 1980s green phosphor CRT terminal (P1 phosphor)
        '--font-family': "'Lucida Console', Monaco, monospace",

        '--color-bg': '#000a00',
        '--color-surface': '#041004',
        '--color-surface-raised': '#081608',
        '--color-surface-hover': '#0c200c',
        '--color-surface-active': '#0a1c0a',

        '--color-border': '#003a00',
        '--color-border-subtle': '#0a1c0a',
        '--color-border-hover': '#00cc00',
        '--color-border-card': '#002a00',

        '--color-text': '#00b800',
        '--color-text-muted': '#006800',
        '--color-text-secondary': '#009000',
        '--color-text-dim': '#00a000',
        '--color-text-bright': '#60ff60',
        '--color-text-placeholder': '#004800',

        '--color-selected': '#001a00',

        '--color-primary': '#00b800',
        '--color-primary-hover': '#00d800',
        '--color-btn-active': '#009800',

        '--color-offline-bg': '#1a0808',
        '--color-offline-text': '#cc4020',

        '--color-stale-bg': '#080e08',
        '--color-stale-text': '#407040',
        '--opacity-stale': '0.5',

        '--color-dup-bg': '#1a1a00',
        '--color-dup-text': '#c0d040',

        '--color-tag-bg': '#001218',
        '--color-tag-text': '#60a0b0',

        '--color-scan-bg': '#001a00',
        '--color-scan-border': '#003a00',
        '--color-scan-text': '#00cc00',
        '--color-progress-track': '#081608',
        '--color-progress-fill': '#00b800',

        '--color-overlay': 'rgba(0, 10, 0, 0.85)',

        '--color-statusbar-bg': '#000800',
        '--color-status-ok': '#00cc00',
        '--color-status-warn': '#c0d040',
        '--color-status-error': '#cc4020',

        // CRT effects — green phosphor glow
        '--text-glow': '0 0 4px rgba(0, 184, 0, 0.35)',
        '--scanline-opacity': '0.6',
        '--title-letter-spacing': '0.15em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 8px rgba(0, 184, 0, 0.5), 0 0 20px rgba(0, 184, 0, 0.15)',
        '--glow-primary': '0 0 6px rgba(0, 184, 0, 0.25), inset 0 0 6px rgba(0, 184, 0, 0.08)',
        '--glow-primary-hover': '0 0 12px rgba(0, 216, 0, 0.35), inset 0 0 10px rgba(0, 216, 0, 0.12)',
        '--glow-focus': '0 0 5px rgba(0, 184, 0, 0.35)',
        '--glow-modal': '0 0 20px rgba(0, 184, 0, 0.12)',
        '--glow-status-dot': '0 0 4px var(--color-status-ok)',
        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(0, 184, 0, 0.08)',
        '--btn-primary-text': '#00b800',

        '--treemap-color-1': '#00aa00',
        '--treemap-color-2': '#008400',
        '--treemap-color-3': '#00c030',
        '--treemap-color-4': '#1a9a1a',
        '--treemap-color-5': '#00b050',
        '--treemap-color-6': '#3a9a00',
        '--treemap-color-7': '#008830',
        '--treemap-color-8': '#50aa00',
    },
    blue: {
        // Blue — dark cobalt, same vibe as red but cold steel blue
        '--font-family': "'Trebuchet MS', 'Lucida Sans', sans-serif",

        '--color-bg': '#040810',
        '--color-surface': '#081018',
        '--color-surface-raised': '#0c141e',
        '--color-surface-hover': '#121e2a',
        '--color-surface-active': '#101a24',

        '--color-border': '#182838',
        '--color-border-subtle': '#101a24',
        '--color-border-hover': '#2266dd',
        '--color-border-card': '#142030',

        '--color-text': '#a8b8d4',
        '--color-text-muted': '#486080',
        '--color-text-secondary': '#6880a0',
        '--color-text-dim': '#8098b8',
        '--color-text-bright': '#e0e8ff',
        '--color-text-placeholder': '#28405a',

        '--color-selected': '#0e1828',

        '--color-primary': '#2266dd',
        '--color-primary-hover': '#3880ee',
        '--color-btn-active': '#1a55bb',

        '--color-offline-bg': '#1a0808',
        '--color-offline-text': '#ff5050',

        '--color-stale-bg': '#0c1020',
        '--color-stale-text': '#6070a0',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#181808',
        '--color-dup-text': '#d4a030',

        '--color-tag-bg': '#081420',
        '--color-tag-text': '#50a0c0',

        '--color-scan-bg': '#081408',
        '--color-scan-border': '#183018',
        '--color-scan-text': '#60c080',
        '--color-progress-track': '#0c141e',
        '--color-progress-fill': '#2266dd',

        '--color-overlay': 'rgba(2, 4, 10, 0.85)',

        '--color-statusbar-bg': '#040608',
        '--color-status-ok': '#60c080',
        '--color-status-warn': '#d4a030',
        '--color-status-error': '#ff5050',

        '--text-glow': '0 0 3px rgba(34, 102, 221, 0.2)',
        '--scanline-opacity': '0',
        '--title-letter-spacing': '0.06em',
        '--title-text-transform': 'uppercase',
        '--title-glow': '0 0 8px rgba(34, 102, 221, 0.5), 0 0 20px rgba(34, 102, 221, 0.15)',
        '--glow-primary': '0 0 6px rgba(34, 102, 221, 0.3), inset 0 0 6px rgba(34, 102, 221, 0.1)',
        '--glow-primary-hover': '0 0 12px rgba(56, 128, 238, 0.4), inset 0 0 10px rgba(56, 128, 238, 0.15)',
        '--glow-focus': '0 0 6px rgba(34, 102, 221, 0.3)',
        '--glow-modal': '0 0 30px rgba(34, 102, 221, 0.12)',
        '--glow-status-dot': '0 0 4px var(--color-status-ok)',
        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(34, 102, 221, 0.1)',
        '--btn-primary-text': '#2266dd',

        '--treemap-color-1': '#3366cc',
        '--treemap-color-2': '#2050aa',
        '--treemap-color-3': '#4488dd',
        '--treemap-color-4': '#2244aa',
        '--treemap-color-5': '#3370bb',
        '--treemap-color-6': '#5588cc',
        '--treemap-color-7': '#2860bb',
        '--treemap-color-8': '#4478cc',
    },

    aqua: {
        // Aqua — light blue/cyan, cool and airy
        '--font-family': "'Segoe UI', Roboto, 'Helvetica Neue', sans-serif",

        '--color-bg': '#041014',
        '--color-surface': '#081820',
        '--color-surface-raised': '#0c1e28',
        '--color-surface-hover': '#142a34',
        '--color-surface-active': '#10242e',

        '--color-border': '#183840',
        '--color-border-subtle': '#10242e',
        '--color-border-hover': '#00b8cc',
        '--color-border-card': '#143038',

        '--color-text': '#a8d0d8',
        '--color-text-muted': '#487880',
        '--color-text-secondary': '#689898',
        '--color-text-dim': '#80b0b8',
        '--color-text-bright': '#e0f4f8',
        '--color-text-placeholder': '#285058',

        '--color-selected': '#0e2028',

        '--color-primary': '#00b8cc',
        '--color-primary-hover': '#20d0e0',
        '--color-btn-active': '#009aaa',

        '--color-offline-bg': '#1a0808',
        '--color-offline-text': '#ff5050',

        '--color-stale-bg': '#081420',
        '--color-stale-text': '#608890',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#181808',
        '--color-dup-text': '#d4a030',

        '--color-tag-bg': '#081820',
        '--color-tag-text': '#40c8e0',

        '--color-scan-bg': '#081408',
        '--color-scan-border': '#183818',
        '--color-scan-text': '#60c080',
        '--color-progress-track': '#0c1e28',
        '--color-progress-fill': '#00b8cc',

        '--color-overlay': 'rgba(2, 8, 10, 0.85)',

        '--color-statusbar-bg': '#040a0c',
        '--color-status-ok': '#60c080',
        '--color-status-warn': '#d4a030',
        '--color-status-error': '#ff5050',

        '--text-glow': '0 0 3px rgba(0, 184, 204, 0.15)',
        '--scanline-opacity': '0',
        '--title-letter-spacing': '0.06em',
        '--title-text-transform': 'none',
        '--title-glow': '0 0 8px rgba(0, 184, 204, 0.4), 0 0 20px rgba(0, 184, 204, 0.1)',
        '--glow-primary': '0 0 6px rgba(0, 184, 204, 0.25), inset 0 0 6px rgba(0, 184, 204, 0.08)',
        '--glow-primary-hover': '0 0 12px rgba(32, 208, 224, 0.35), inset 0 0 10px rgba(32, 208, 224, 0.12)',
        '--glow-focus': '0 0 6px rgba(0, 184, 204, 0.3)',
        '--glow-modal': '0 0 30px rgba(0, 184, 204, 0.1)',
        '--glow-status-dot': '0 0 4px var(--color-status-ok)',
        '--btn-primary-bg': 'transparent',
        '--btn-primary-bg-hover': 'rgba(0, 184, 204, 0.08)',
        '--btn-primary-text': '#00b8cc',

        '--treemap-color-1': '#00a0b0',
        '--treemap-color-2': '#008898',
        '--treemap-color-3': '#20b8c8',
        '--treemap-color-4': '#007888',
        '--treemap-color-5': '#00c0a0',
        '--treemap-color-6': '#40a8b8',
        '--treemap-color-7': '#009090',
        '--treemap-color-8': '#30b0a0',
    },

    'light blue': {
        // Light Blue — light theme with cool cyan/blue accents
        '--font-family': "'Segoe UI', Roboto, 'Helvetica Neue', sans-serif",

        '--color-bg': '#e4eff6',
        '--color-surface': '#f2f8fc',
        '--color-surface-raised': '#eaf4fa',
        '--color-surface-hover': '#d6e8f2',
        '--color-surface-active': '#cce2ee',

        '--color-border': '#a8c8dc',
        '--color-border-subtle': '#c8dcea',
        '--color-border-hover': '#0892b0',
        '--color-border-card': '#b4d0e0',

        '--color-text': '#1a2c38',
        '--color-text-muted': '#607888',
        '--color-text-secondary': '#3c5868',
        '--color-text-dim': '#2e4858',
        '--color-text-bright': '#0c1820',
        '--color-text-placeholder': '#90a8b8',

        '--color-selected': '#c8e4f4',

        '--color-primary': '#0892b0',
        '--color-primary-hover': '#067a96',
        '--color-btn-active': '#056880',

        '--color-offline-bg': '#fcecec',
        '--color-offline-text': '#c82828',

        '--color-stale-bg': '#e8f0f6',
        '--color-stale-text': '#608898',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#f8f4e0',
        '--color-dup-text': '#886800',

        '--color-tag-bg': '#dceef6',
        '--color-tag-text': '#186878',

        '--color-scan-bg': '#e8f6ec',
        '--color-scan-border': '#a8d4b0',
        '--color-scan-text': '#287830',
        '--color-progress-track': '#c8dcea',
        '--color-progress-fill': '#0892b0',

        '--color-overlay': 'rgba(10, 24, 36, 0.35)',

        '--color-statusbar-bg': '#dce8f0',
        '--color-status-ok': '#287830',
        '--color-status-warn': '#886800',
        '--color-status-error': '#c82828',

        '--text-glow': 'none',
        '--scanline-opacity': '0',
        '--title-glow': 'none',
        '--glow-primary': 'none',
        '--glow-primary-hover': 'none',
        '--glow-focus': '0 0 4px rgba(8, 146, 176, 0.3)',
        '--glow-modal': 'none',
        '--glow-stat': 'none',
        '--glow-status-dot': 'none',
        '--glow-dup': 'none',
        '--btn-primary-bg': 'var(--color-primary)',
        '--btn-primary-bg-hover': 'var(--color-primary-hover)',
        '--btn-primary-text': '#ffffff',

        '--treemap-color-1': '#0892b0',
        '--treemap-color-2': '#287830',
        '--treemap-color-3': '#4868a8',
        '--treemap-color-4': '#c06030',
        '--treemap-color-5': '#18a0a0',
        '--treemap-color-6': '#b84848',
        '--treemap-color-7': '#987020',
        '--treemap-color-8': '#6870b0',
    },

    corporate: {
        // Corporate — clean, neutral, professional. No glow, no flair.
        '--font-family': "'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",

        '--color-bg': '#f4f5f7',
        '--color-surface': '#ffffff',
        '--color-surface-raised': '#f8f9fa',
        '--color-surface-hover': '#e9ecef',
        '--color-surface-active': '#dee2e6',

        '--color-border': '#ced4da',
        '--color-border-subtle': '#e9ecef',
        '--color-border-hover': '#0d6efd',
        '--color-border-card': '#dee2e6',

        '--color-text': '#212529',
        '--color-text-muted': '#6c757d',
        '--color-text-secondary': '#495057',
        '--color-text-dim': '#343a40',
        '--color-text-bright': '#000000',
        '--color-text-placeholder': '#adb5bd',

        '--color-selected': '#e7f1ff',

        '--color-primary': '#0d6efd',
        '--color-primary-hover': '#0b5ed7',
        '--color-btn-active': '#0a58ca',

        '--color-offline-bg': '#fff5f5',
        '--color-offline-text': '#dc3545',

        '--color-stale-bg': '#f8f9fa',
        '--color-stale-text': '#6c757d',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#fff8e1',
        '--color-dup-text': '#856404',

        '--color-tag-bg': '#e7f1ff',
        '--color-tag-text': '#0d6efd',

        '--color-scan-bg': '#f0fdf4',
        '--color-scan-border': '#b7e1cd',
        '--color-scan-text': '#198754',
        '--color-progress-track': '#e9ecef',
        '--color-progress-fill': '#0d6efd',

        '--color-overlay': 'rgba(0, 0, 0, 0.4)',

        '--color-statusbar-bg': '#e9ecef',
        '--color-status-ok': '#198754',
        '--color-status-warn': '#ffc107',
        '--color-status-error': '#dc3545',

        '--text-glow': 'none',
        '--scanline-opacity': '0',
        '--title-glow': 'none',
        '--glow-primary': 'none',
        '--glow-primary-hover': 'none',
        '--glow-focus': '0 0 4px rgba(13, 110, 253, 0.3)',
        '--glow-modal': 'none',
        '--glow-stat': 'none',
        '--glow-status-dot': 'none',
        '--glow-dup': 'none',
        '--btn-primary-bg': 'var(--color-primary)',
        '--btn-primary-bg-hover': 'var(--color-primary-hover)',
        '--btn-primary-text': '#ffffff',

        '--treemap-color-1': '#0d6efd',
        '--treemap-color-2': '#198754',
        '--treemap-color-3': '#6f42c1',
        '--treemap-color-4': '#fd7e14',
        '--treemap-color-5': '#20c997',
        '--treemap-color-6': '#dc3545',
        '--treemap-color-7': '#ffc107',
        '--treemap-color-8': '#0dcaf0',
    },

    'default light': {
        // Default Light — the default dark theme flipped to light
        '--font-family': "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",

        '--color-bg': '#e8e8f0',
        '--color-surface': '#f4f4fa',
        '--color-surface-raised': '#eeeef6',
        '--color-surface-hover': '#dcdce8',
        '--color-surface-active': '#d4d4e2',

        '--color-border': '#c0c0d0',
        '--color-border-subtle': '#d8d8e4',
        '--color-border-hover': '#5b8def',
        '--color-border-card': '#ccccd8',

        '--color-text': '#2a2a38',
        '--color-text-muted': '#707088',
        '--color-text-secondary': '#505068',
        '--color-text-dim': '#404058',
        '--color-text-bright': '#1a1a28',
        '--color-text-placeholder': '#9898a8',

        '--color-selected': '#d8e4ff',

        '--color-primary': '#4a78d8',
        '--color-primary-hover': '#3a68c8',
        '--color-btn-active': '#3060b8',

        '--color-offline-bg': '#fceaea',
        '--color-offline-text': '#cc2828',

        '--color-stale-bg': '#ece8f4',
        '--color-stale-text': '#7060a0',
        '--opacity-stale': '0.55',

        '--color-dup-bg': '#f8f4e0',
        '--color-dup-text': '#906800',

        '--color-tag-bg': '#e0ecfc',
        '--color-tag-text': '#3868c0',

        '--color-scan-bg': '#e8f8ec',
        '--color-scan-border': '#b0d8b8',
        '--color-scan-text': '#287030',
        '--color-progress-track': '#d4d4e2',
        '--color-progress-fill': '#4a78d8',

        '--color-overlay': 'rgba(0, 0, 0, 0.3)',

        '--color-statusbar-bg': '#e0e0ec',
        '--color-status-ok': '#287030',
        '--color-status-warn': '#906800',
        '--color-status-error': '#cc2828',

        '--text-glow': 'none',
        '--scanline-opacity': '0',
        '--title-glow': 'none',
        '--glow-primary': 'none',
        '--glow-primary-hover': 'none',
        '--glow-focus': '0 0 4px rgba(74, 120, 216, 0.3)',
        '--glow-modal': 'none',
        '--glow-stat': 'none',
        '--glow-status-dot': 'none',
        '--glow-dup': 'none',
        '--btn-primary-bg': 'var(--color-primary)',
        '--btn-primary-bg-hover': 'var(--color-primary-hover)',
        '--btn-primary-text': '#ffffff',

        '--treemap-color-1': '#4a78d8',
        '--treemap-color-2': '#38a870',
        '--treemap-color-3': '#7858c0',
        '--treemap-color-4': '#c86030',
        '--treemap-color-5': '#48a048',
        '--treemap-color-6': '#c04848',
        '--treemap-color-7': '#a08020',
        '--treemap-color-8': '#8060b0',
    },
};

export const themeNames = Object.keys(themes);

export function applyTheme(name) {
    const vars = themes[name];
    if (!vars) return;

    // Reset — remove all inline overrides so brand.css defaults take effect
    document.documentElement.removeAttribute('style');

    // Apply theme-specific overrides
    for (const [prop, value] of Object.entries(vars)) {
        document.documentElement.style.setProperty(prop, value);
    }

    localStorage.setItem('fh-theme', name);
    localStorage.setItem('fh-theme-vars', JSON.stringify(vars));
}

// Backfill fh-theme-vars for existing users and apply theme if the
// inline head script couldn't (first load after update, or default theme).
const saved = localStorage.getItem('fh-theme');
if (saved && themes[saved]) {
    applyTheme(saved);
}

export default themes;
