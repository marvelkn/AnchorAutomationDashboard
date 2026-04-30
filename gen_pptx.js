const pptxgen = require("pptxgenjs");

const OUT = "C:\\Users\\Lenovo\\Documents\\UMN\\Semester 6 Magang\\Project Magang\\AnchorAutomationDashboard\\ML_Pipeline_Presentasi.pptx";

// ── Palette ───────────────────────────────────────────────────────────────────
const C = {
  DARK:       "0F2044",  // dark navy – title / dark slides
  NAVY:       "1A3A6B",  // medium navy – header bars
  BLUE:       "1A56DB",  // vivid blue – accents, icons
  AMBER:      "F59E0B",  // amber – callouts / highlights
  LIGHT_BG:   "F0F4FA",  // near-white – content slides
  CARD:       "FFFFFF",  // white cards
  TEXT_DARK:  "1E293B",
  TEXT_MID:   "475569",
  TEXT_LIGHT: "E2E8F0",
  TEXT_WHITE: "FFFFFF",
  SUCCESS:    "059669",
  WARNING:    "D97706",
  DANGER:     "DC2626",
  BORDER:     "CBD5E1",
};

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";
pres.title  = "ML Pipeline – BTN Anchor Automation Dashboard";
pres.author = "BTN Data Team";

// ── Helpers ───────────────────────────────────────────────────────────────────
function darkSlide(s) { s.background = { color: C.DARK }; }
function lightSlide(s) { s.background = { color: C.LIGHT_BG }; }

/** Top header bar with title text */
function addHeader(s, text, sub) {
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 10, h: 0.72,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addText(text, {
    x: 0.4, y: 0, w: 9.2, h: 0.72,
    fontSize: 18, bold: true, color: C.TEXT_WHITE,
    valign: "middle", align: "left", margin: 0,
  });
  if (sub) {
    s.addText(sub, {
      x: 0.4, y: 0.68, w: 9.2, h: 0.32,
      fontSize: 10, color: C.BLUE, italic: true,
      valign: "middle", align: "left", margin: 0,
    });
  }
}

/** Amber left-bar card */
function addCard(s, x, y, w, h, title, body, titleSize = 11, bodySize = 9.5) {
  s.addShape(pres.shapes.RECTANGLE, {
    x, y, w, h,
    fill: { color: C.CARD },
    shadow: { type: "outer", blur: 8, offset: 2, angle: 135, color: "000000", opacity: 0.10 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x, y, w: 0.06, h,
    fill: { color: C.AMBER }, line: { color: C.AMBER },
  });
  if (title) {
    s.addText(title, {
      x: x + 0.12, y, w: w - 0.18, h: 0.3,
      fontSize: titleSize, bold: true, color: C.NAVY,
      valign: "bottom", margin: 0,
    });
  }
  if (body) {
    s.addText(body, {
      x: x + 0.12, y: y + 0.32, w: w - 0.18, h: h - 0.36,
      fontSize: bodySize, color: C.TEXT_DARK,
      valign: "top", margin: 0, wrap: true,
    });
  }
}

/** Numbered circle */
function addCircle(s, cx, cy, r, num) {
  s.addShape(pres.shapes.OVAL, {
    x: cx - r, y: cy - r, w: r * 2, h: r * 2,
    fill: { color: C.BLUE }, line: { color: C.BLUE },
  });
  s.addText(String(num), {
    x: cx - r, y: cy - r, w: r * 2, h: r * 2,
    fontSize: 14, bold: true, color: C.TEXT_WHITE,
    align: "center", valign: "middle", margin: 0,
  });
}

/** Horizontal arrow */
function addArrow(s, x, y, w) {
  s.addShape(pres.shapes.LINE, {
    x, y, w, h: 0,
    line: { color: C.BLUE, width: 2 },
  });
  // small arrowhead triangle (approximated with a tiny right triangle shape)
  s.addShape(pres.shapes.RECTANGLE, {
    x: x + w - 0.01, y: y - 0.08, w: 0.01, h: 0.16,
    fill: { color: C.BLUE }, line: { color: C.BLUE },
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 1 — TITLE
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  darkSlide(s);

  // Decorative shapes
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.35, h: 5.625,
    fill: { color: C.BLUE }, line: { color: C.BLUE },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 4.8, w: 10, h: 0.825,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.35, y: 2.2, w: 9.65, h: 0.06,
    fill: { color: C.AMBER }, line: { color: C.AMBER },
  });

  s.addText("Machine Learning Pipeline", {
    x: 0.65, y: 0.8, w: 9, h: 0.8,
    fontSize: 36, bold: true, color: C.TEXT_WHITE,
    align: "left", valign: "middle", margin: 0,
  });
  s.addText("BTN Anchor Automation Dashboard", {
    x: 0.65, y: 1.6, w: 9, h: 0.55,
    fontSize: 22, bold: false, color: C.TEXT_LIGHT,
    align: "left", valign: "middle", margin: 0,
  });
  s.addText("Deteksi Dini Risiko Churn & Segmentasi Performa Merchant", {
    x: 0.65, y: 2.35, w: 9, h: 0.45,
    fontSize: 13, color: C.AMBER, italic: true,
    align: "left", valign: "middle", margin: 0,
  });

  s.addText("Bank BTN  ·  Data & Analytics Team  ·  2026", {
    x: 0.65, y: 4.9, w: 9, h: 0.5,
    fontSize: 10, color: C.TEXT_LIGHT,
    align: "left", valign: "middle", margin: 0,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 2 — GAMBARAN BESAR PIPELINE
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Gambaran Besar Pipeline", "Enam tahap dari data mentah ke keputusan risiko");

  const steps = [
    { n: 1, label: "Data\nMerge",       sub: "3 tabel ETL\ndigabung" },
    { n: 2, label: "Feature\nEngineering", sub: "Normalise, clip,\nlog, scale" },
    { n: 3, label: "K-Means++\nClustering", sub: "Segmentasi\nperforma tier" },
    { n: 4, label: "Isolation\nForest",  sub: "Deteksi\nanomali" },
    { n: 5, label: "MAD\nZ-Score",       sub: "Ukur jarak\ndari median" },
    { n: 6, label: "Risk Score\n& Klasifikasi", sub: "0–100 → HIGH /\nMEDIUM / STABLE" },
  ];

  const boxW = 1.38, boxH = 1.4, startX = 0.25, boxY = 1.5, gap = 0.08;
  steps.forEach((st, i) => {
    const bx = startX + i * (boxW + gap);
    // card
    s.addShape(pres.shapes.RECTANGLE, {
      x: bx, y: boxY, w: boxW, h: boxH,
      fill: { color: C.CARD },
      shadow: { type: "outer", blur: 8, offset: 2, angle: 135, color: "000000", opacity: 0.12 },
    });
    // colored top strip
    s.addShape(pres.shapes.RECTANGLE, {
      x: bx, y: boxY, w: boxW, h: 0.28,
      fill: { color: i < 2 ? C.NAVY : i < 4 ? C.BLUE : C.AMBER },
      line: { color: i < 2 ? C.NAVY : i < 4 ? C.BLUE : C.AMBER },
    });
    // number
    s.addText(String(st.n), {
      x: bx, y: boxY, w: boxW, h: 0.28,
      fontSize: 11, bold: true, color: C.TEXT_WHITE,
      align: "center", valign: "middle", margin: 0,
    });
    // label
    s.addText(st.label, {
      x: bx + 0.05, y: boxY + 0.32, w: boxW - 0.1, h: 0.58,
      fontSize: 11, bold: true, color: C.NAVY,
      align: "center", valign: "middle", margin: 0,
    });
    // sub
    s.addText(st.sub, {
      x: bx + 0.05, y: boxY + 0.9, w: boxW - 0.1, h: 0.46,
      fontSize: 8.5, color: C.TEXT_MID,
      align: "center", valign: "top", margin: 0,
    });
    // arrow between boxes
    if (i < steps.length - 1) {
      s.addShape(pres.shapes.LINE, {
        x: bx + boxW + 0.01, y: boxY + boxH / 2, w: gap + 0.06, h: 0,
        line: { color: C.BLUE, width: 1.5 },
      });
    }
  });

  // bottom note
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.25, y: 3.15, w: 9.5, h: 0.55,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addText("Pipeline berjalan setiap kali halaman Dashboard dibuka — model dilatih ulang dari data terbaru, tanpa file model tersimpan.", {
    x: 0.35, y: 3.15, w: 9.3, h: 0.55,
    fontSize: 10, color: C.TEXT_WHITE,
    align: "left", valign: "middle", margin: 0,
  });

  // 3b sub-note for parallel
  s.addText("Tahap 3a (K-Means) dan 3b (Isolation Forest) berjalan PARALEL — dua perspektif berbeda pada data yang sama.", {
    x: 0.25, y: 3.85, w: 9.5, h: 0.4,
    fontSize: 9, color: C.TEXT_MID, italic: true,
    align: "center", valign: "middle", margin: 0,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 3 — TANTANGAN DATA
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Tantangan: Data Mentah Tidak Bisa Langsung Dipakai",
    "Tiga masalah utama yang harus diselesaikan sebelum ML");

  const rows = [
    ["Data Mentah",         "Masalah",                                         "Solusi"],
    ["TOTAL_SV (akumulasi YTD)",
     "Merchant aktif 3 bulan vs 12 bulan tidak bisa dibandingkan langsung",
     "÷ months_active → AVG_SV"],
    ["SV_GROWTH_RATE",
     "Satu outlier ekstrem (mis. +1000%) bisa mendistorsi seluruh model",
     "Clip di P5–P95 → SV_GROWTH_CLIPPED"],
    ["TOTAL_SV: Rp 100M vs Rp 10T",
     "Skala terlalu jauh — model akan terlalu fokus pada merchant besar",
     "Log transform (log1p) → skala dikompresi"],
    ["Tidak ada kolom achievement",
     "Tidak ada cara mengukur seberapa jauh merchant dari target 2026",
     "Dibuat baru: ACHIEVEMENT_PCT = TOTAL_SV / TARGET × 100"],
  ];

  const colW = [2.2, 3.8, 3.4];
  const rowH = 0.68;
  const startY = 0.82;

  rows.forEach((row, ri) => {
    const isHeader = ri === 0;
    let cx = 0.25;
    row.forEach((cell, ci) => {
      const fillColor = isHeader ? C.NAVY : ri % 2 === 0 ? "EEF2FF" : C.CARD;
      s.addShape(pres.shapes.RECTANGLE, {
        x: cx, y: startY + ri * rowH, w: colW[ci], h: rowH,
        fill: { color: fillColor },
        line: { color: C.BORDER, width: 0.5 },
      });
      s.addText(cell, {
        x: cx + 0.1, y: startY + ri * rowH, w: colW[ci] - 0.2, h: rowH,
        fontSize: isHeader ? 10.5 : 9.5,
        bold: isHeader,
        color: isHeader ? C.TEXT_WHITE : ci === 2 ? C.BLUE : C.TEXT_DARK,
        valign: "middle", margin: 0, wrap: true,
      });
      cx += colW[ci];
    });
  });

  s.addText("Tanpa feature engineering ini, model ML akan menghasilkan segmentasi yang bias dan tidak bisa ditindaklanjuti.", {
    x: 0.25, y: 4.55, w: 9.5, h: 0.4,
    fontSize: 9.5, color: C.TEXT_MID, italic: true,
    align: "center", valign: "middle", margin: 0,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 4 — FEATURE ENGINEERING
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Feature Engineering: Membuat Data Bisa Dibandingkan",
    "Tiga langkah transformasi → 6 fitur final");

  const steps = [
    {
      n: 1, color: C.NAVY,
      title: "Normalisasi per Bulan Aktif",
      body: "AVG_SV  =  TOTAL_SV ÷ months_active\nAVG_FBI  =  TOTAL_FBI ÷ months_active\n\nmonths_active = WEEKS_ACTIVE ÷ 4.33 (clip 1–12)\n\nSemua total dibagi durasi aktif sebenarnya — bukan selalu ÷ 12.",
    },
    {
      n: 2, color: C.BLUE,
      title: "Log Transform & Clipping",
      body: "log1p(AVG_SV), log1p(AVG_FBI)\n→ Kompresi rentang nilai besar\n\nSV_GROWTH_CLIPPED: potong di P5 & P95\nACHIEVEMENT_PCT: clip 0–200%\nRASIO_ONUS: clip 0–1",
    },
    {
      n: 3, color: C.AMBER,
      title: "Z-Normalisasi (StandardScaler)",
      body: "Semua 6 fitur diskala ke mean=0, std=1\n\n6 Fitur Final:\nAVG_SV · AVG_FBI · RASIO_ONUS\nSV_GROWTH_CLIPPED · ACHIEVEMENT_PCT\nWEEKS_ACTIVE",
    },
  ];

  const cardW = 2.95, cardH = 2.85, startX = 0.2, cardY = 0.9, gap = 0.225;
  steps.forEach((st, i) => {
    const cx = startX + i * (cardW + gap);
    s.addShape(pres.shapes.RECTANGLE, {
      x: cx, y: cardY, w: cardW, h: cardH,
      fill: { color: C.CARD },
      shadow: { type: "outer", blur: 10, offset: 3, angle: 135, color: "000000", opacity: 0.12 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: cx, y: cardY, w: cardW, h: 0.55,
      fill: { color: st.color }, line: { color: st.color },
    });
    s.addText(`${st.n}`, {
      x: cx, y: cardY, w: 0.55, h: 0.55,
      fontSize: 16, bold: true, color: C.TEXT_WHITE,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(st.title, {
      x: cx + 0.6, y: cardY, w: cardW - 0.65, h: 0.55,
      fontSize: 10.5, bold: true, color: C.TEXT_WHITE,
      valign: "middle", margin: 0, wrap: true,
    });
    s.addText(st.body, {
      x: cx + 0.15, y: cardY + 0.62, w: cardW - 0.3, h: cardH - 0.7,
      fontSize: 9.5, color: C.TEXT_DARK, lineSpacingMultiple: 1.2,
      valign: "top", margin: 0, wrap: true,
    });
    // Arrow
    if (i < steps.length - 1) {
      s.addShape(pres.shapes.LINE, {
        x: cx + cardW + 0.03, y: cardY + cardH / 2, w: gap - 0.06, h: 0,
        line: { color: C.BLUE, width: 2 },
      });
    }
  });

  s.addText("Analogi: Membandingkan IPK mahasiswa S1 4 tahun vs semester 1 — harus dinormalisasi per semester terlebih dahulu.", {
    x: 0.25, y: 3.95, w: 9.5, h: 0.4,
    fontSize: 9.5, color: C.TEXT_MID, italic: true,
    align: "center", valign: "middle", margin: 0,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 5 — K-MEANS++
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "K-Means++: Segmentasi Performa Merchant",
    "Model tidak diberitahu seperti apa PREMIUM itu — ia menemukan sendiri");

  // Left: how it works
  const items = [
    { icon: "⬛", label: "Input", text: "6 fitur yang sudah diskala per merchant" },
    { icon: "⬛", label: "Algoritma", text: "K-Means++ (inisialisasi centroid lebih cerdas)\nn_init=20, random_state=42" },
    { icon: "⬛", label: "Output", text: "K klaster alami (K = 3, 4, atau 5 — dapat diatur PM)" },
    { icon: "⬛", label: "Penamaan", text: "Klaster diberi nama berdasarkan skor komposit:\nSV 60% + Achievement 25% + Growth 15%" },
  ];

  items.forEach((it, i) => {
    addCard(s, 0.3, 0.9 + i * 1.0, 4.7, 0.85, it.label, it.text, 10, 9);
  });

  // Right: label map
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.25, y: 0.9, w: 4.5, h: 0.45,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addText("Label Map per Nilai K", {
    x: 5.25, y: 0.9, w: 4.5, h: 0.45,
    fontSize: 10.5, bold: true, color: C.TEXT_WHITE,
    align: "center", valign: "middle", margin: 0,
  });

  const labelRows = [
    ["K", "Labels (Tertinggi → Terendah"],
    ["3", "PREMIUM → REGULER → PASIF"],
    ["4", "ELITE → PREMIUM → REGULER → PASIF"],
    ["5", "ELITE → PREMIUM → REGULER → PASIF → DORMANT"],
  ];
  labelRows.forEach((row, ri) => {
    const isH = ri === 0;
    const ry = 1.35 + ri * 0.5;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 5.25, y: ry, w: 4.5, h: 0.48,
      fill: { color: isH ? "E8EDF8" : C.CARD },
      line: { color: C.BORDER, width: 0.5 },
    });
    s.addText(row[0], {
      x: 5.25, y: ry, w: 0.55, h: 0.48,
      fontSize: 10, bold: isH, color: isH ? C.NAVY : C.BLUE,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(row[1], {
      x: 5.82, y: ry, w: 3.9, h: 0.48,
      fontSize: isH ? 9 : 9.5, bold: isH, color: C.TEXT_DARK,
      valign: "middle", margin: 0,
    });
  });

  // Silhouette explainer
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.25, y: 3.45, w: 4.5, h: 1.3,
    fill: { color: C.CARD },
    shadow: { type: "outer", blur: 8, offset: 2, angle: 135, color: "000000", opacity: 0.10 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.25, y: 3.45, w: 4.5, h: 0.3,
    fill: { color: C.AMBER }, line: { color: C.AMBER },
  });
  s.addText("Silhouette Score — Validasi Kualitas Klaster", {
    x: 5.35, y: 3.45, w: 4.3, h: 0.3,
    fontSize: 9.5, bold: true, color: C.TEXT_WHITE,
    valign: "middle", margin: 0,
  });
  const ssRows = [
    ["> 0.5",     "Pemisahan klaster KUAT — K yang dipilih bagus"],
    ["0.25–0.5",  "Pemisahan SEDANG — dapat diterima"],
    ["< 0.25",    "Pemisahan LEMAH — coba K yang berbeda"],
  ];
  const ssColors = [C.SUCCESS, C.WARNING, C.DANGER];
  ssRows.forEach((r, i) => {
    s.addShape(pres.shapes.RECTANGLE, {
      x: 5.25, y: 3.77 + i * 0.31, w: 0.9, h: 0.3,
      fill: { color: ssColors[i] }, line: { color: ssColors[i] },
    });
    s.addText(r[0], {
      x: 5.25, y: 3.77 + i * 0.31, w: 0.9, h: 0.3,
      fontSize: 9, bold: true, color: C.TEXT_WHITE,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(r[1], {
      x: 6.2, y: 3.77 + i * 0.31, w: 3.5, h: 0.3,
      fontSize: 9, color: C.TEXT_DARK,
      valign: "middle", margin: 0,
    });
  });

  s.addText("\"Model tidak diberitahu seperti apa merchant PREMIUM — ia menemukan pengelompokan yang memang ada di dalam data.\"", {
    x: 0.3, y: 4.95, w: 9.4, h: 0.38,
    fontSize: 9.5, color: C.NAVY, italic: true,
    align: "center", valign: "middle", margin: 0,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 6 — ISOLATION FOREST
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Isolation Forest: Deteksi Merchant yang Anomali",
    "\"Merchant yang aneh lebih mudah diisolasi dalam kerumunan\"");

  // Left column: how it works
  const leftCards = [
    {
      title: "Ide Dasar",
      body: "Acak-acak pertanyaan pemilahan data berulang kali.\nMerchant ANEH = tertangkap lebih cepat (path lebih pendek).\nMerchant NORMAL = butuh lebih banyak pertanyaan.",
    },
    {
      title: "Parameter",
      body: "n_estimators: 100 pohon acak\ncontamination: 0.10 (~10% portofolio ≈ 3–4 merchant)\nBekerja pada 6 fitur SECARA BERSAMAAN",
    },
    {
      title: "Keunggulan vs Threshold Satu Dimensi",
      body: "Volume normal + Growth normal + On-us ratio tinggi + Minggu aktif sedikit\n→ Kombinasinya anomali, tapi tidak terdeteksi jika dicek satu per satu.",
    },
  ];
  leftCards.forEach((c, i) => {
    addCard(s, 0.3, 0.9 + i * 1.15, 4.6, 1.0, c.title, c.body, 10.5, 9);
  });

  // Right: LOFO explainer
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.15, y: 0.9, w: 4.6, h: 0.42,
    fill: { color: C.BLUE }, line: { color: C.BLUE },
  });
  s.addText("LOFO — Leave-One-Feature-Out", {
    x: 5.15, y: 0.9, w: 4.6, h: 0.42,
    fontSize: 11, bold: true, color: C.TEXT_WHITE,
    align: "center", valign: "middle", margin: 0,
  });
  s.addText(
    "Untuk setiap fitur:\n1. Netralkan fitur (set ke nilai rata-rata portofolio)\n2. Hitung ulang skor anomali TANPA re-training\n3. Delta = skor_dasar − skor_netral\n\nDelta positif besar → fitur ITU yang menyebabkan flag anomali.\n\nHasil: Isolation Forest yang bisa dijelaskan — PM tahu tepat\nmengapa merchant ini diflag.",
    {
      x: 5.25, y: 1.37, w: 4.4, h: 2.0,
      fontSize: 9.5, color: C.TEXT_DARK, lineSpacingMultiple: 1.3,
      valign: "top", margin: 0, wrap: true,
    }
  );

  // Output columns
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.15, y: 3.45, w: 4.6, h: 0.32,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addText("Output per Merchant", {
    x: 5.15, y: 3.45, w: 4.6, h: 0.32,
    fontSize: 9.5, bold: true, color: C.TEXT_WHITE,
    align: "center", valign: "middle", margin: 0,
  });
  const outputs = [
    "IF_ANOMALY_SCORE  — skor kontinu (makin tinggi makin anomali)",
    "IF_IS_ANOMALY     — flag boolean (True = ~10% teratas)",
    "IF_CONTRIB_*      — kontribusi LOFO per fitur (6 kolom)",
  ];
  outputs.forEach((o, i) => {
    s.addText(o, {
      x: 5.25, y: 3.82 + i * 0.34, w: 4.4, h: 0.32,
      fontSize: 9, color: C.TEXT_DARK, fontFace: "Consolas",
      valign: "middle", margin: 0,
    });
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 7 — MAD Z-SCORE
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "MAD Z-Score: Mengukur Jarak dari Norma Portofolio",
    "Lebih andal dari Z-Score standar untuk portofolio kecil 38 merchant");

  // Two comparison cards
  const cards = [
    {
      x: 0.3, color: C.DANGER, label: "❌  Z-Score Standar (Mean)",
      body: "Formula: Z = (x − mean) / std\n\nMasalah: Mean dipengaruhi outlier.\nDalam portofolio 38 merchant, SATU merchant\nekstrem bisa menggeser mean secara signifikan\n→ semua merchant lain terlihat lebih aman\ndari yang sebenarnya.",
    },
    {
      x: 5.1, color: C.SUCCESS, label: "✓  MAD Z-Score (Median)",
      body: "Formula: Z = 0.6745 × (x − median) / MAD\nMAD = median(|x − median(x)|)\n\nKeunggulan: Median tidak terpengaruh outlier.\nJauh lebih stabil dan andal untuk portofolio\nkecil seperti ini.",
    },
  ];
  cards.forEach(c => {
    s.addShape(pres.shapes.RECTANGLE, {
      x: c.x, y: 0.85, w: 4.6, h: 2.8,
      fill: { color: C.CARD },
      shadow: { type: "outer", blur: 10, offset: 2, angle: 135, color: "000000", opacity: 0.10 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: c.x, y: 0.85, w: 4.6, h: 0.44,
      fill: { color: c.color }, line: { color: c.color },
    });
    s.addText(c.label, {
      x: c.x + 0.1, y: 0.85, w: 4.4, h: 0.44,
      fontSize: 11, bold: true, color: C.TEXT_WHITE,
      valign: "middle", margin: 0,
    });
    s.addText(c.body, {
      x: c.x + 0.15, y: 1.35, w: 4.3, h: 2.25,
      fontSize: 10, color: C.TEXT_DARK, lineSpacingMultiple: 1.3,
      valign: "top", margin: 0, wrap: true,
    });
  });

  // Three dimensions
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.3, y: 3.85, w: 9.4, h: 0.38,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addText("Diterapkan pada 3 dimensi (setelah log transform):", {
    x: 0.4, y: 3.85, w: 9.2, h: 0.38,
    fontSize: 10, bold: true, color: C.TEXT_WHITE,
    valign: "middle", margin: 0,
  });

  const dims = [
    { label: "ZSCORE_SV",     desc: "log(AVG_SV) — anomali volume penjualan" },
    { label: "ZSCORE_FBI",    desc: "log(AVG_FBI) — anomali fee income" },
    { label: "ZSCORE_GROWTH", desc: "SV_GROWTH_CLIPPED — tren pertumbuhan" },
  ];
  dims.forEach((d, i) => {
    const dx = 0.3 + i * 3.17;
    s.addShape(pres.shapes.RECTANGLE, {
      x: dx, y: 4.28, w: 3.1, h: 0.7,
      fill: { color: "EEF2FF" }, line: { color: C.BORDER, width: 0.5 },
    });
    s.addText(d.label, {
      x: dx + 0.1, y: 4.28, w: 2.9, h: 0.3,
      fontSize: 10, bold: true, color: C.NAVY, fontFace: "Consolas",
      valign: "bottom", margin: 0,
    });
    s.addText(d.desc, {
      x: dx + 0.1, y: 4.58, w: 2.9, h: 0.36,
      fontSize: 9, color: C.TEXT_MID,
      valign: "top", margin: 0,
    });
  });

  s.addText("Nilai negatif = merchant di bawah median portofolio untuk metrik tersebut.", {
    x: 0.3, y: 5.08, w: 9.4, h: 0.32,
    fontSize: 9, color: C.TEXT_MID, italic: true,
    align: "center", valign: "middle", margin: 0,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 8 — RISK SCORE KOMPOSIT
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Risk Score Komposit: Menyatukan Semua Sinyal",
    "Satu angka 0–100 yang mencerminkan tingkat risiko churn");

  // Formula box
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.3, y: 0.85, w: 5.8, h: 2.5,
    fill: { color: "F8FAFC" }, line: { color: C.BORDER, width: 1 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.3, y: 0.85, w: 5.8, h: 0.38,
    fill: { color: C.DARK }, line: { color: C.DARK },
  });
  s.addText("Formula RISK_SCORE", {
    x: 0.3, y: 0.85, w: 5.8, h: 0.38,
    fontSize: 11, bold: true, color: C.TEXT_WHITE,
    align: "center", valign: "middle", margin: 0,
  });
  s.addText(
    "clip(−ZSCORE_GROWTH, 0, 3) / 3  ×  40   ← Tren pertumbuhan\n" +
    "+ clip(−ZSCORE_SV,     0, 3) / 3  ×  30   ← Volume penjualan\n" +
    "+ clip(−ZSCORE_FBI,    0, 3) / 3  ×  20   ← Fee income\n" +
    "+ clip(1 − ACHV/100,   0, 1)      ×  10   ← Gap target\n\n" +
    "Result clipped to [0, 100]",
    {
      x: 0.45, y: 1.27, w: 5.5, h: 2.0,
      fontSize: 10, fontFace: "Consolas", color: C.TEXT_DARK, lineSpacingMultiple: 1.4,
      valign: "top", margin: 0,
    }
  );

  // Weight rationale cards
  const weights = [
    { pct: "40%", label: "Tren Pertumbuhan", color: C.DANGER,
      why: "Penurunan trajectory = sinyal churn terkuat. Merchant yang akan pergi biasanya menurun lebih dulu sebelum volume jatuh." },
    { pct: "30%", label: "Volume Penjualan", color: C.WARNING,
      why: "Dropping sales = kehilangan bisnis. Sinyal terkuat kedua." },
    { pct: "20%", label: "Fee Income", color: C.BLUE,
      why: "Mengikuti volume. Penurunan FBI tanpa penurunan SV menandakan pergeseran produk." },
    { pct: "10%", label: "Gap Target", color: "6B7280",
      why: "Bobotnya paling kecil karena tidak semua merchant memiliki target yang terdefinisi." },
  ];

  weights.forEach((w, i) => {
    const wy = 0.85 + i * 1.15;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 6.35, y: wy, w: 3.3, h: 1.0,
      fill: { color: C.CARD },
      shadow: { type: "outer", blur: 6, offset: 2, angle: 135, color: "000000", opacity: 0.09 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: 6.35, y: wy, w: 0.8, h: 1.0,
      fill: { color: w.color }, line: { color: w.color },
    });
    s.addText(w.pct, {
      x: 6.35, y: wy, w: 0.8, h: 1.0,
      fontSize: 14, bold: true, color: C.TEXT_WHITE,
      align: "center", valign: "middle", margin: 0,
    });
    s.addText(w.label, {
      x: 7.22, y: wy + 0.05, w: 2.35, h: 0.3,
      fontSize: 10, bold: true, color: C.TEXT_DARK,
      valign: "middle", margin: 0,
    });
    s.addText(w.why, {
      x: 7.22, y: wy + 0.35, w: 2.35, h: 0.6,
      fontSize: 8.5, color: C.TEXT_MID,
      valign: "top", margin: 0, wrap: true,
    });
  });

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.3, y: 3.48, w: 5.8, h: 0.5,
    fill: { color: C.AMBER }, line: { color: C.AMBER },
  });
  s.addText("Semakin tinggi RISK_SCORE, semakin besar risiko churn. Score 0 = sempurna, 100 = kritis.", {
    x: 0.4, y: 3.48, w: 5.6, h: 0.5,
    fontSize: 10, bold: true, color: C.DARK,
    valign: "middle", margin: 0, wrap: true,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 9 — KLASIFIKASI RISIKO & ENSEMBLE ALERT
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Klasifikasi Risiko & Ensemble Alert",
    "Tiga kategori + dua lapisan pengaman tambahan");

  const tiers = [
    { range: "≥ 60", label: "HIGH RISK ⚠️",   color: C.DANGER,  text: "Intervensi segera. Merchant menunjukkan penurunan signifikan di beberapa dimensi sekaligus." },
    { range: "30–59",label: "MEDIUM RISK 🟡", color: C.WARNING, text: "Pemantauan intensif. Ada sinyal awal penurunan yang perlu ditindaklanjuti." },
    { range: "< 30", label: "STABLE ✅",       color: C.SUCCESS, text: "Performa normal. Pemantauan rutin sudah cukup." },
  ];

  tiers.forEach((t, i) => {
    const ty = 0.88 + i * 1.1;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.3, y: ty, w: 1.1, h: 1.0,
      fill: { color: t.color }, line: { color: t.color },
    });
    s.addText(t.range, {
      x: 0.3, y: ty, w: 1.1, h: 1.0,
      fontSize: 14, bold: true, color: C.TEXT_WHITE,
      align: "center", valign: "middle", margin: 0,
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: 1.42, y: ty, w: 4.1, h: 1.0,
      fill: { color: C.CARD }, line: { color: C.BORDER, width: 0.5 },
    });
    s.addText(t.label, {
      x: 1.55, y: ty + 0.05, w: 3.9, h: 0.32,
      fontSize: 11, bold: true, color: t.color,
      valign: "middle", margin: 0,
    });
    s.addText(t.text, {
      x: 1.55, y: ty + 0.38, w: 3.9, h: 0.56,
      fontSize: 9.5, color: C.TEXT_DARK,
      valign: "top", margin: 0, wrap: true,
    });
  });

  // Z-Score Tripwire
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.8, y: 0.88, w: 3.9, h: 1.55,
    fill: { color: C.CARD },
    shadow: { type: "outer", blur: 8, offset: 2, angle: 135, color: "000000", opacity: 0.10 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.8, y: 0.88, w: 3.9, h: 0.4,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addText("Z-Score Tripwire (Override)", {
    x: 5.9, y: 0.88, w: 3.7, h: 0.4,
    fontSize: 10.5, bold: true, color: C.TEXT_WHITE,
    valign: "middle", margin: 0,
  });
  s.addText(
    "Jika merchant STABLE tapi salah satu dimensi Z-Score < threshold (default −1.2):\n→ Otomatis dinaikkan ke MEDIUM RISK\n\nMencegah sinyal bahaya parah pada satu dimensi tersembunyi oleh dua dimensi lain yang masih baik.",
    {
      x: 5.9, y: 1.32, w: 3.7, h: 1.07,
      fontSize: 9, color: C.TEXT_DARK, lineSpacingMultiple: 1.25,
      valign: "top", margin: 0, wrap: true,
    }
  );

  // Ensemble Alert
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.8, y: 2.58, w: 3.9, h: 1.65,
    fill: { color: C.DARK },
    shadow: { type: "outer", blur: 10, offset: 3, angle: 135, color: "000000", opacity: 0.2 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 5.8, y: 2.58, w: 3.9, h: 0.4,
    fill: { color: C.AMBER }, line: { color: C.AMBER },
  });
  s.addText("⚡ ENSEMBLE ALERT — Prioritas Tertinggi", {
    x: 5.9, y: 2.58, w: 3.7, h: 0.4,
    fontSize: 10, bold: true, color: C.DARK,
    valign: "middle", margin: 0,
  });
  s.addText(
    "Merchant mendapat flag dari DUA metode sekaligus:\n• HIGH RISK dari Risk Score Komposit\n• Anomali dari Isolation Forest\n\nDua model berbeda, dua pendekatan berbeda,\nmenunjuk merchant yang sama.\n→ Tingkat keyakinan JAUH lebih tinggi.",
    {
      x: 5.9, y: 3.02, w: 3.7, h: 1.17,
      fontSize: 9.5, color: C.TEXT_LIGHT, lineSpacingMultiple: 1.3,
      valign: "top", margin: 0, wrap: true,
    }
  );

  // PM control note
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.3, y: 4.5, w: 9.4, h: 0.72,
    fill: { color: "EEF2FF" }, line: { color: C.BORDER, width: 0.5 },
  });
  s.addText("PM dapat mengatur sensitivitas melalui slider Threshold Z-Score di dashboard — mengontrol seberapa agresif sistem meng-upgrade STABLE ke MEDIUM RISK tanpa harus menyentuh kode.", {
    x: 0.45, y: 4.5, w: 9.1, h: 0.72,
    fontSize: 9.5, color: C.NAVY, italic: true,
    valign: "middle", margin: 0, wrap: true,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 10 — HOLT-WINTERS FORECASTING
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Holt-Winters: Proyeksi Penjualan Per Merchant",
    "Modul terpisah — berjalan on-demand, per merchant");

  // Chart illustration using shapes
  const chartX = 0.3, chartY = 0.88, chartW = 5.8, chartH = 3.5;
  s.addShape(pres.shapes.RECTANGLE, {
    x: chartX, y: chartY, w: chartW, h: chartH,
    fill: { color: C.CARD }, line: { color: C.BORDER, width: 1 },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: chartX, y: chartY, w: chartW, h: 0.38,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addText("Ilustrasi Output Holt-Winters", {
    x: chartX, y: chartY, w: chartW, h: 0.38,
    fontSize: 10, bold: true, color: C.TEXT_WHITE,
    align: "center", valign: "middle", margin: 0,
  });

  // Axes
  s.addShape(pres.shapes.LINE, { x: chartX+0.5, y: chartY+3.15, w: chartW-0.7, h: 0, line: { color: "94A3B8", width: 1 } });
  s.addShape(pres.shapes.LINE, { x: chartX+0.5, y: chartY+0.55, w: 0, h: 2.62, line: { color: "94A3B8", width: 1 } });

  // Fake historical line (solid)
  const histPts = [[0,2.0],[0.6,1.8],[1.1,2.3],[1.6,1.6],[2.1,2.1],[2.6,1.5],[3.1,2.4],[3.55,1.9]];
  for (let i = 0; i < histPts.length - 1; i++) {
    const [x1,y1] = histPts[i], [x2,y2] = histPts[i+1];
    s.addShape(pres.shapes.LINE, {
      x: chartX + 0.5 + x1, y: chartY + 0.55 + y1, w: x2-x1, h: y2-y1,
      line: { color: C.BLUE, width: 2 },
    });
  }

  // Forecast line (dashed, color = amber)
  const fcPts = [[3.55,1.9],[3.9,1.6],[4.2,2.0],[4.55,1.5],[4.85,1.3]];
  for (let i = 0; i < fcPts.length - 1; i++) {
    const [x1,y1] = fcPts[i], [x2,y2] = fcPts[i+1];
    s.addShape(pres.shapes.LINE, {
      x: chartX + 0.5 + x1, y: chartY + 0.55 + y1, w: x2-x1, h: y2-y1,
      line: { color: C.AMBER, width: 2, dashType: "dash" },
    });
  }

  // Divider line at forecast start
  s.addShape(pres.shapes.LINE, {
    x: chartX + 0.5 + 3.55, y: chartY + 0.55, w: 0, h: 2.62,
    line: { color: "94A3B8", width: 1, dashType: "sysDot" },
  });

  s.addText("Historis", { x: chartX+0.55, y: chartY+3.05, w: 1.5, h: 0.25, fontSize: 8, color: C.BLUE, margin: 0 });
  s.addText("Proyeksi", { x: chartX+4.15, y: chartY+3.05, w: 1.5, h: 0.25, fontSize: 8, color: C.AMBER, margin: 0 });
  s.addText("Sekarang →", { x: chartX+3.5, y: chartY+0.58, w: 1.0, h: 0.25, fontSize: 7.5, color: "94A3B8", margin: 0 });

  // Right: algorithm tiers
  const tiers = [
    { cond: "≥ 24 bulan data", algo: "Holt-Winters Penuh", detail: "Tren + dekomposisi musiman (seasonal_periods=12)" },
    { cond: "6–23 bulan data", algo: "Holt's Double Smoothing", detail: "Tren saja — tanpa komponen musiman" },
    { cond: "< 6 bulan data",  algo: "Linear Extrapolation", detail: "Fallback sederhana — hasil kurang akurat" },
  ];

  s.addShape(pres.shapes.RECTANGLE, {
    x: 6.35, y: 0.88, w: 3.4, h: 0.4,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });
  s.addText("Algoritma Otomatis berdasarkan Data", {
    x: 6.35, y: 0.88, w: 3.4, h: 0.4,
    fontSize: 9.5, bold: true, color: C.TEXT_WHITE,
    align: "center", valign: "middle", margin: 0,
  });

  tiers.forEach((t, i) => {
    const ty = 1.33 + i * 1.0;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 6.35, y: ty, w: 3.4, h: 0.9,
      fill: { color: C.CARD }, line: { color: C.BORDER, width: 0.5 },
    });
    const dotColor = i === 0 ? C.SUCCESS : i === 1 ? C.WARNING : C.DANGER;
    s.addShape(pres.shapes.OVAL, { x: 6.38, y: ty+0.08, w: 0.18, h: 0.18, fill: { color: dotColor }, line: { color: dotColor } });
    s.addText(t.cond, { x: 6.62, y: ty+0.03, w: 3.05, h: 0.25, fontSize: 9, bold: true, color: C.TEXT_DARK, valign: "middle", margin: 0 });
    s.addText(t.algo, { x: 6.62, y: ty+0.26, w: 3.05, h: 0.25, fontSize: 9.5, bold: true, color: C.NAVY, valign: "middle", margin: 0 });
    s.addText(t.detail, { x: 6.42, y: ty+0.52, w: 3.25, h: 0.35, fontSize: 8.5, color: C.TEXT_MID, valign: "top", margin: 0, wrap: true });
  });

  // Output note
  s.addShape(pres.shapes.RECTANGLE, {
    x: 6.35, y: 4.38, w: 3.4, h: 0.84,
    fill: { color: "EEF2FF" }, line: { color: C.BORDER, width: 0.5 },
  });
  s.addText("Output:\n• Grafik historis + proyeksi\n• Multiplier musiman\n• Verdict: on track / at risk / critical", {
    x: 6.45, y: 4.4, w: 3.2, h: 0.8,
    fontSize: 9, color: C.NAVY,
    valign: "top", margin: 0, wrap: true,
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 11 — KENAPA DUA MODEL?
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Kenapa K-Means DAN Isolation Forest?",
    "Dua pertanyaan berbeda, dua model berbeda — bersama lebih lengkap");

  // Comparison header
  const cols = [
    { x: 0.3,  color: C.NAVY, label: "K-Means++",        q: "\"Merchant ini termasuk kelompok mana?\"" },
    { x: 5.15, color: C.BLUE, label: "Isolation Forest",  q: "\"Apakah merchant ini berperilaku aneh untuk kelompoknya?\"" },
  ];
  cols.forEach(c => {
    s.addShape(pres.shapes.RECTANGLE, {
      x: c.x, y: 0.88, w: 4.65, h: 0.55,
      fill: { color: c.color }, line: { color: c.color },
    });
    s.addText(c.label, {
      x: c.x + 0.1, y: 0.88, w: 4.45, h: 0.28,
      fontSize: 12, bold: true, color: C.TEXT_WHITE,
      valign: "middle", margin: 0,
    });
    s.addText(c.q, {
      x: c.x + 0.1, y: 1.16, w: 4.45, h: 0.27,
      fontSize: 9, color: C.TEXT_WHITE, italic: true,
      valign: "middle", margin: 0,
    });
  });

  // 2x2 matrix
  const mX = 0.3, mY = 1.55, cellW = 4.65, cellH = 1.8;
  const matrixData = [
    // [topLeft, topRight, bottomLeft, bottomRight]
    {
      x: mX, y: mY, label: "PREMIUM — Tidak Anomali",
      color: "F0FDF4", border: C.SUCCESS,
      body: "Merchant sehat performa tinggi.\n→ Pantau rutin, pertahankan hubungan.",
    },
    {
      x: mX + cellW + 0.1, y: mY, label: "PREMIUM — Anomali ⚡",
      color: "FFF7ED", border: C.DANGER,
      body: "PRIORITAS TINGGI.\nAda perubahan drastis pada merchant top.\n→ Investigasi segera.",
    },
    {
      x: mX, y: mY + cellH + 0.08, label: "PASIF — Tidak Anomali",
      color: "F8FAFC", border: "94A3B8",
      body: "Performa rendah tapi konsisten.\n→ Program aktivasi jangka panjang.",
    },
    {
      x: mX + cellW + 0.1, y: mY + cellH + 0.08, label: "PASIF — Anomali",
      color: "FEF3C7", border: C.WARNING,
      body: "Merchant pasif berperilaku tak terduga.\n→ Perhatian berbeda dari PREMIUM anomali.",
    },
  ];

  matrixData.forEach(m => {
    s.addShape(pres.shapes.RECTANGLE, {
      x: m.x, y: m.y, w: cellW, h: cellH,
      fill: { color: m.color }, line: { color: m.border, width: 1.5 },
    });
    s.addText(m.label, {
      x: m.x + 0.15, y: m.y + 0.1, w: cellW - 0.3, h: 0.35,
      fontSize: 11, bold: true, color: C.TEXT_DARK,
      valign: "middle", margin: 0,
    });
    s.addText(m.body, {
      x: m.x + 0.15, y: m.y + 0.48, w: cellW - 0.3, h: cellH - 0.55,
      fontSize: 10, color: C.TEXT_MID, lineSpacingMultiple: 1.3,
      valign: "top", margin: 0, wrap: true,
    });
  });

  // axis labels
  s.addText("← Cluster Tier: Tinggi", { x: 0.3, y: 5.18, w: 4.65, h: 0.25, fontSize: 8.5, color: C.TEXT_MID, align: "center", margin: 0 });
  s.addText("Anomali (Isolation Forest) →", { x: 5.15, y: 5.18, w: 4.65, h: 0.25, fontSize: 8.5, color: C.TEXT_MID, align: "center", margin: 0 });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 12 — PERTANYAAN UMUM
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  lightSlide(s);
  addHeader(s, "Antisipasi Pertanyaan Audiens",
    "Pertanyaan yang paling sering muncul beserta jawabannya");

  const qas = [
    {
      q: "Kenapa tidak pakai model supervised (klasifikasi)?",
      a: "Tidak ada data historis merchant yang sudah churn. Tanpa label training, supervised learning tidak bisa digunakan. K-Means dan Isolation Forest bekerja tanpa label.",
    },
    {
      q: "Kenapa contamination 10% di Isolation Forest?",
      a: "10% dari 38 merchant = ~4 merchant. Cukup kecil untuk bisa diinvestigasi PM, tidak terlalu besar sampai menjadi noise yang diabaikan.",
    },
    {
      q: "Apakah model bisa salah?",
      a: "Ya, bisa. Itulah mengapa PM tetap punya kontrol: slider sensitivity, LOFO contribution, dan Ensemble Alert. Model memberi sinyal — PM yang memutuskan.",
    },
    {
      q: "Seberapa sering model dilatih ulang?",
      a: "Setiap kali halaman Dashboard dibuka. Tidak ada file model tersimpan — selalu fresh dari data terbaru, di-cache selama satu sesi untuk performa.",
    },
    {
      q: "Apa bedanya MAD Z-Score dengan Z-Score biasa?",
      a: "Z-Score biasa memakai mean yang dipengaruhi outlier. MAD memakai median yang stabil. Untuk portofolio kecil 38 merchant, MAD jauh lebih andal.",
    },
  ];

  qas.forEach((qa, i) => {
    const qy = 0.88 + i * 0.92;
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.3, y: qy, w: 9.4, h: 0.86,
      fill: { color: C.CARD },
      shadow: { type: "outer", blur: 6, offset: 2, angle: 135, color: "000000", opacity: 0.08 },
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.3, y: qy, w: 0.32, h: 0.86,
      fill: { color: i % 2 === 0 ? C.BLUE : C.AMBER },
      line: { color: i % 2 === 0 ? C.BLUE : C.AMBER },
    });
    s.addText(`Q${i+1}  ${qa.q}`, {
      x: 0.7, y: qy + 0.04, w: 8.9, h: 0.32,
      fontSize: 10, bold: true, color: C.NAVY,
      valign: "middle", margin: 0, wrap: true,
    });
    s.addText(qa.a, {
      x: 0.7, y: qy + 0.38, w: 8.9, h: 0.44,
      fontSize: 9.5, color: C.TEXT_MID,
      valign: "top", margin: 0, wrap: true,
    });
  });
}

// ═════════════════════════════════════════════════════════════════════════════
// SLIDE 13 — KESIMPULAN
// ═════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  darkSlide(s);

  s.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.35, h: 5.625,
    fill: { color: C.AMBER }, line: { color: C.AMBER },
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.35, y: 4.8, w: 9.65, h: 0.825,
    fill: { color: C.NAVY }, line: { color: C.NAVY },
  });

  s.addText("Kesimpulan & Nilai Bisnis", {
    x: 0.65, y: 0.5, w: 9, h: 0.6,
    fontSize: 26, bold: true, color: C.TEXT_WHITE,
    valign: "middle", margin: 0,
  });
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.65, y: 1.15, w: 8.8, h: 0.05,
    fill: { color: C.AMBER }, line: { color: C.AMBER },
  });

  const points = [
    { icon: "▶", text: "Menjawab satu pertanyaan bisnis: merchant mana yang perlu diperhatikan sekarang, dan mengapa?" },
    { icon: "▶", text: "Setiap keputusan teknis (MAD Z-Score, LOFO, Ensemble Alert) dirancang untuk menghasilkan sinyal yang actionable bagi PM." },
    { icon: "▶", text: "Bukan prediksi biner yang bisa salah/benar — ini sistem peringatan dini yang memberi PM waktu bereaksi sebelum churn terjadi." },
    { icon: "▶", text: "Model dilatih ulang otomatis dari data terbaru setiap sesi — tidak ada drift dari model lama yang kadaluwarsa." },
  ];
  points.forEach((p, i) => {
    s.addText(`${p.icon}  ${p.text}`, {
      x: 0.65, y: 1.35 + i * 0.72, w: 8.8, h: 0.65,
      fontSize: 13, color: C.TEXT_LIGHT, lineSpacingMultiple: 1.2,
      valign: "middle", margin: 0, wrap: true,
    });
  });

  s.addText("Bank BTN  ·  Data & Analytics Team  ·  2026", {
    x: 0.65, y: 4.88, w: 8.8, h: 0.46,
    fontSize: 10, color: C.TEXT_LIGHT,
    align: "left", valign: "middle", margin: 0,
  });
}

// ── Write ─────────────────────────────────────────────────────────────────────
pres.writeFile({ fileName: OUT })
  .then(() => console.log("✅  Saved:", OUT))
  .catch(e => { console.error("❌ Error:", e); process.exit(1); });
