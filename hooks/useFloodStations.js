import { useEffect, useState } from "react";

// URL negeri
const STATION_URLS = {
  PLS: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PLS&district=ALL&station=ALL&lang=en",
  KDH: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=KDH&district=ALL&station=ALL&lang=en",
  PNG: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PNG&district=ALL&station=ALL&lang=en",
  PRK: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=PRK&district=ALL&station=ALL&lang=en",
  SGR: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/?state=SGR&district=ALL&station=ALL&lang=en",

  MLK: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=MLK&district=ALL&station=ALL&lang=en",
  JHR: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=JHR&district=ALL&station=ALL&lang=en",
  PHG: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=PHG&district=ALL&station=ALL&lang=en",
  TRG: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=TRG&district=ALL&station=ALL&lang=en",
  KTN: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=KTN&district=ALL&station=ALL&lang=en",
  SWK: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=SWK&district=ALL&station=ALL&lang=en",
  SBH: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=SBH&district=ALL&station=ALL&lang=en",
  LBN: "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air-data/?state=LBN&district=ALL&station=ALL&lang=en",
};

// --- Normalizer untuk JSON endpoint ---
function normalizeStations(raw, stateCode) {
  return (raw?.data ?? []).map((s, i) => ({
    id: `${stateCode}-${s.id ?? i}`,
    type: "station",
    name: s.station ?? s.name ?? "Station",
    status: (s.status ?? "normal").toLowerCase(),
    level: parseFloat(s.water_level ?? s.level ?? 0),
    river: s.river ?? "",
    district: s.district ?? "",
    coordinate: {
      latitude: parseFloat(s.lat ?? s.latitude ?? 0),
      longitude: parseFloat(s.lon ?? s.longitude ?? 0),
    },
    raw: s,
  }));
}

// --- Parser untuk HTML table ---
function parseHtmlTable(html, state) {
  const rows = [...html.matchAll(/<tr[^>]*>(.*?)<\/tr>/g)];
  const stations = [];

  rows.forEach((row, i) => {
    const cells = [...row[1].matchAll(/<td[^>]*>(.*?)<\/td>/g)].map(
      (c) => c[1].replace(/<[^>]+>/g, "").trim()
    );
    if (cells.length < 11) return; // skip header / row tak lengkap

    stations.push({
      id: `${state}-${cells[0]}`, // Station ID
      type: "station",
      name: cells[1], // Station Name
      district: cells[2],
      river: cells[3],
      subRiver: cells[4],
      lastUpdated: cells[5],
      level: parseFloat(cells[6]) || 0,
      thresholds: {
        normal: parseFloat(cells[7]) || 0,
        alert: parseFloat(cells[8]) || 0,
        warning: parseFloat(cells[9]) || 0,
        danger: parseFloat(cells[10]) || 0,
      },
      coordinate: null, // HTML table tiada lat/lon
      raw: cells,
    });
  });

  return stations;
}

// --- Fetch setiap negeri ---
async function fetchStation(url, state) {
  try {
    const res = await fetch(url);
    const text = await res.text();

    // cuba parse sebagai JSON
    try {
      const json = JSON.parse(text);
      return normalizeStations(json, state);
    } catch {
      // fallback HTML
      return parseHtmlTable(text, state);
    }
  } catch (err) {
    console.warn(`❌ ${state} gagal`, err.message);
    return [];
  }
}

// --- Hook utama ---
export default function useFloodStations() {
  const [stations, setStations] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchAll() {
      setLoading(true);
      let allStations = [];
      for (const [state, url] of Object.entries(STATION_URLS)) {
        const s = await fetchStation(url, state);
        allStations = [...allStations, ...s];
      }
      setStations(allStations);
      setLoading(false);
    }
    fetchAll();
  }, []);

  return { stations, loading };
}
