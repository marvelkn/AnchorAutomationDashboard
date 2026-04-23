# Dashboard Frontend Styling Guide

A reference for all custom HTML/CSS patterns used in this Streamlit dashboard.
Copy-paste ready — just swap in your data variables.

---

## 1. Left-Accent Cards (Flex Row)

Used for: **Merchant Tier cards**, **Account Manager cards**

Each card has a colored left border, large number, progress bar, and a pill status chip.
Cards sit in a horizontal flex row and wrap automatically on smaller screens.

```python
_pp = _p()  # theme palette helper — provides TEXT_PRI, TEXT_SEC, BORDER

cards_html = '<div style="display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap;">'

for item in items:
    c     = "#2F80ED"          # accent color — unique per item
    count = 42
    pct   = 55.3               # percentage for progress bar (0–100)
    label = "merchants"
    sub_label = "Achievement"
    sub_value = 86             # shown in color on progress bar
    sub_color = '#34D399' if sub_value >= 80 else ('#FBBF24' if sub_value >= 50 else '#F87171')
    bar_w = min(sub_value, 100)

    # Status chip — green or red pill
    status_chip = (
        f'<div style="display:inline-block;background:#F8717122;border:1px solid #F87171;'
        f'border-radius:20px;padding:2px 9px;font-size:0.68rem;color:#F87171;'
        f'font-weight:700;margin-top:6px;">⚠️ 3 High Risk</div>'
        # --- OR ---
        f'<div style="display:inline-block;background:#34D39922;border:1px solid #34D399;'
        f'border-radius:20px;padding:2px 9px;font-size:0.68rem;color:#34D399;'
        f'font-weight:700;margin-top:6px;">✅ All on track</div>'
    )

    cards_html += (
        f'<div style="flex:1;min-width:150px;border-left:5px solid {c};'
        f'background:{c}14;border-radius:0 14px 14px 0;padding:16px 18px;">'

        # Header label (icon + name)
        f'<div style="font-size:0.65rem;font-weight:800;text-transform:uppercase;'
        f'letter-spacing:.09em;color:{c};">👤 ITEM NAME</div>'

        # Big number
        f'<div style="font-size:2.4rem;font-weight:900;color:{_pp["TEXT_PRI"]};'
        f'line-height:1;margin:6px 0 2px;">{count}</div>'

        # Sub-label under number
        f'<div style="font-size:0.78rem;color:{_pp["TEXT_SEC"]};margin-bottom:10px;">{label}</div>'

        # Progress bar label
        f'<div style="font-size:0.69rem;color:{_pp["TEXT_SEC"]};margin-bottom:3px;">'
        f'{sub_label}: <span style="color:{sub_color};font-weight:700;">{sub_value:.0f}%</span></div>'

        # Progress bar track + fill
        f'<div style="height:4px;border-radius:2px;background:{_pp["BORDER"]};margin-bottom:8px;">'
        f'<div style="width:{bar_w:.1f}%;height:100%;border-radius:2px;background:{sub_color};"></div>'
        f'</div>'

        # Status chip
        f'{status_chip}'
        f'</div>'
    )

cards_html += '</div>'
st.markdown(cards_html, unsafe_allow_html=True)
```

### Color Palette for Per-Item Colors
When each card needs a distinct color (e.g. one per AM), use this palette:
```python
PALETTE = ['#2F80ED', '#9B59B6', '#F39C12', '#1ABC9C', '#E67E22', '#16A085']
c = PALETTE[i % len(PALETTE)]
```

### Tier-Specific Colors (fixed by tier name)
```python
TIER_COLORS = {
    'ELITE':   '#F1C40F',
    'PREMIUM': '#27AE60',
    'REGULER': '#2F80ED',
    'PASIF':   '#EB5757',
    'DORMANT': '#888888',
}
c = TIER_COLORS.get(tier_name, '#888888')
```

---

## 2. Fleet % Progress Bar (inside a card)

Used inside the tier cards — a thin track + colored fill showing share of total.

```python
pct     = 44.7                # percentage value
bar_clr = '#EB5757'           # match the card's accent color

f'<div style="height:4px;border-radius:2px;background:{_pp["BORDER"]};margin-bottom:8px;">'
f'<div style="width:{min(pct, 100):.1f}%;height:100%;border-radius:2px;background:{bar_clr};"></div>'
f'</div>'
```

---

## 3. High-Risk Warning Chip

Used inside cards when a count > 0. Inline pill, no layout impact.

```python
# Red — warning
f'<div style="display:inline-block;background:#F8717122;border:1px solid #F87171;'
f'border-radius:20px;padding:2px 9px;font-size:0.68rem;color:#F87171;'
f'font-weight:700;margin-top:6px;">⚠️ {n} High Risk</div>'

# Green — all clear
f'<div style="display:inline-block;background:#34D39922;border:1px solid #34D399;'
f'border-radius:20px;padding:2px 9px;font-size:0.68rem;color:#34D399;'
f'font-weight:700;margin-top:6px;">✅ All on track</div>'

# Amber — at risk
f'<div style="display:inline-block;background:#FBBF2422;border:1px solid #FBBF24;'
f'border-radius:20px;padding:2px 9px;font-size:0.68rem;color:#FBBF24;'
f'font-weight:700;margin-top:6px;">🟡 AT RISK</div>'
```

---

## 4. Horizontal Stat Strip (Aggregate Summary)

Used for: **Active AMs / Avg Merchant Load / Unassigned Merchants**

Different from per-item cards — centered layout, pill container, vertical dividers.
Use this when showing fleet-level totals, not individual breakdowns.

```python
_pp = _p()

# Conditional color for an alert cell (e.g. unassigned merchants)
alert_val   = 5
alert_color = '#FBBF24' if alert_val > 0 else '#34D399'
alert_bg    = '#FBBF2414' if alert_val > 0 else '#34D39914'
alert_sub   = f'+{alert_val} need assignment' if alert_val > 0 else 'fully assigned ✓'

divider = f'border-right:1px solid {_pp["BORDER"]};'

st.markdown(
    f"""<div style="display:flex;border:1px solid {_pp['BORDER']};
        border-radius:14px;overflow:hidden;margin:12px 0 18px;">

      <!-- Cell 1 — neutral -->
      <div style="flex:1;text-align:center;padding:18px 12px;{divider}">
        <div style="font-size:0.65rem;font-weight:700;text-transform:uppercase;
                    letter-spacing:.08em;color:{_pp['TEXT_SEC']};margin-bottom:6px;">
          👥 Active Account Managers</div>
        <div style="font-size:2.2rem;font-weight:900;
                    color:{_pp['TEXT_PRI']};line-height:1;">4</div>
        <div style="font-size:0.74rem;color:{_pp['TEXT_SEC']};margin-top:4px;">
          PMs managing portfolio</div>
      </div>

      <!-- Cell 2 — neutral -->
      <div style="flex:1;text-align:center;padding:18px 12px;{divider}">
        <div style="font-size:0.65rem;font-weight:700;text-transform:uppercase;
                    letter-spacing:.08em;color:{_pp['TEXT_SEC']};margin-bottom:6px;">
          ⚖️ Avg Merchant Load</div>
        <div style="font-size:2.2rem;font-weight:900;
                    color:{_pp['TEXT_PRI']};line-height:1;">10.2</div>
        <div style="font-size:0.74rem;color:{_pp['TEXT_SEC']};margin-top:4px;">
          merchants per AM</div>
      </div>

      <!-- Cell 3 — conditional alert color -->
      <div style="flex:1;text-align:center;padding:18px 12px;background:{alert_bg};">
        <div style="font-size:0.65rem;font-weight:700;text-transform:uppercase;
                    letter-spacing:.08em;color:{alert_color};margin-bottom:6px;">
          📋 Unassigned Merchants</div>
        <div style="font-size:2.2rem;font-weight:900;
                    color:{alert_color};line-height:1;">{alert_val}</div>
        <div style="font-size:0.74rem;color:{alert_color};margin-top:4px;font-weight:600;">
          {alert_sub}</div>
      </div>

    </div>""",
    unsafe_allow_html=True
)
```

---

## 5. Status Box (AI Insight Summary)

Used for: **AI Insight Summary status — ON TRACK / AT RISK / CRITICAL**

Left-border box with tinted background. Color driven by a percentage threshold.

```python
rate_pct   = proj_eoy / fy_target * 100   # achievement %

exec_color = '#34D399' if rate_pct >= 100 else ('#FBBF24' if rate_pct >= 80 else '#F87171')
exec_icon  = '🟢'       if rate_pct >= 100 else ('🟡'       if rate_pct >= 80 else '🔴')
exec_label = 'ON TRACK' if rate_pct >= 100 else ('AT RISK'  if rate_pct >= 80 else 'CRITICAL — INTERVENTION REQUIRED')

_pp = _p()

st.markdown(
    f"""<div style="border-left:5px solid {exec_color};background:{exec_color}18;
        border-radius:0 12px 12px 0;padding:16px 20px;margin-bottom:14px;">
        <div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;
                    letter-spacing:.08em;color:{exec_color};">
          {exec_icon} STATUS: {exec_label}</div>
        <div style="font-size:0.88rem;margin-top:8px;color:{_pp['TEXT_PRI']};line-height:1.65;">
          Narrative text goes here.
        </div>
    </div>""",
    unsafe_allow_html=True
)
```

---

## 6. Summary Action Table (below tier cards)

Used after the tier cards to show all tiers in one scannable table.

```python
import pandas as pd

rows = []
for seg in all_clusters:
    rows.append({
        'Tier':               f'{icon} {seg}',
        'Merchants':          n,
        '% Fleet':            f'{pct:.1f}%',
        '⚠️ High Risk':       high_in_seg if high_in_seg > 0 else '—',
        'Recommended Action': action_text,
    })

df_table = pd.DataFrame(rows)
st.dataframe(
    df_table, hide_index=True, use_container_width=True,
    column_config={
        'Tier':               st.column_config.TextColumn('Tier',               width='small'),
        'Merchants':          st.column_config.NumberColumn('Merchants',         width='small'),
        '% Fleet':            st.column_config.TextColumn('Fleet %',            width='small'),
        '⚠️ High Risk':       st.column_config.TextColumn('⚠️ High Risk',       width='small'),
        'Recommended Action': st.column_config.TextColumn('Recommended Action'),
    }
)
```

---

## 7. Plotly Bar Chart — Show Y-Axis Labels

Used for: **Top 5 Gainers / Losers** (horizontal bar charts)

Key: `showticklabels=True` + `automargin=True` so Plotly auto-sizes the left space.

```python
import plotly.graph_objects as go

fig = go.Figure(go.Bar(
    x=df['value'],
    y=df['label'],          # truncated merchant names, e.g. df['_Label']
    orientation='h',
    marker_color='#27AE60',
    text=[f"Rp {v/1e6:,.0f}Jt ({r:.0f}%)" for v, r in zip(df['value'], df['pct'])],
    textposition='inside',
    insidetextanchor='middle',
))

fig.update_layout(
    height=280,
    margin=dict(l=0, r=20, t=10, b=40),   # l=0, let automargin handle left space
    xaxis={'title': 'Volume Change (Jt Rp)'},
    yaxis=dict(
        showgrid=False,
        automargin=True,        # auto-expands left margin to fit label text
        showticklabels=True,    # THIS was the fix — was False before
        tickfont=dict(size=11),
    ),
)
st.plotly_chart(fig, use_container_width=True, theme=None)
```

### Truncate long merchant names for y-axis
```python
def _trunc_name(n, limit=15):
    s = str(n)
    return (s[:limit] + '…') if len(s) > limit else s

df['_Label'] = df['MERCHANT_GROUP'].apply(_trunc_name)
```

---

## Quick Reference: Color Tokens

| Meaning        | Color     | Hex       |
|----------------|-----------|-----------|
| Success/Green  | Emerald   | `#34D399` |
| Warning/Amber  | Amber     | `#FBBF24` |
| Danger/Red     | Red       | `#F87171` |
| Info/Blue      | Blue      | `#2F80ED` |
| Elite/Gold     | Yellow    | `#F1C40F` |
| Premium/Green  | Green     | `#27AE60` |
| Reguler/Blue   | Blue      | `#2F80ED` |
| Pasif/Red      | Red       | `#EB5757` |
| Dormant/Gray   | Gray      | `#888888` |

### Tint formula (for card backgrounds)
Append `14` (≈8% opacity) or `18` (≈10% opacity) or `22` (≈13% opacity) to any hex color:
```
#2F80ED14   →  very light blue background
#F8717122   →  very light red background (used for chip badges)
```

---

## Quick Reference: Typography Scale

| Usage                  | `font-size`  | `font-weight` |
|------------------------|--------------|---------------|
| Big stat number        | `2.4rem`     | `900`         |
| Medium stat number     | `2.2rem`     | `900`         |
| Card sub-label         | `0.78rem`    | `400`         |
| Chip / badge text      | `0.68rem`    | `700`         |
| Section header label   | `0.65rem`    | `800`         |
| Progress bar sub-text  | `0.69rem`    | `400`         |
| Status box narrative   | `0.88rem`    | `400`         |

---

## Pattern Decision Guide

| Situation | Use |
|-----------|-----|
| Per-item breakdown (one card per entity) | Left-accent card (§1) |
| Fleet/portfolio totals | Horizontal stat strip (§4) |
| Pass/fail status with narrative | Status box (§5) |
| All items compared in one view | Summary action table (§6) |
| Horizontal bar chart with names | Plotly with `showticklabels=True` (§7) |
