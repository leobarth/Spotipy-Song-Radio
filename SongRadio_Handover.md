# SongRadio – Session Handover

Zusammenfassung einer längeren Optimierungs-/Feature-Session an `SongRadio.py`
(Spotify + Last.fm "Radio"-Empfehlungstool: nimmt einen Seed-Track, baut einen
Kandidatenpool über Genre-/Artist-/Similar-Artist-Suche auf, filtert und
rankt via Last.fm-Daten, erstellt optional eine Spotify-Playlist).

Stand am Ende der Session: Datei kompiliert fehlerfrei, alle Fixes unten sind
funktional getestet (Mock-basierte Unit-/Integrationstests, keine echten
API-Calls). `main.py` wurde nie angefasst/gesehen – sie ist laut Nutzer ein
dünner Wrapper ohne relevante Logik.

---

## 1. Architektur-Kurzreferenz

**Pipeline (aufgerufen von `main.py`, nicht editiert):**
1. `resolve_seed_track_ids()` – Spotify-Suche nach dem Seed-Track
2. `get_seed_artist_ids_and_genres()` – lädt Seed-Track(s), ermittelt
   `seed_genre_set` primär via Last.fm-Tags
3. `build_candidate_pool(genre_counter)` – Kandidaten aus drei Quellen
   (Genre-Suche, Seed-Artist-Katalog, Last.fm-Similar-Artists), plus
   Snowball-Expansion-Runden
4. `filter_and_rank(candidates)` – Last.fm-Filterkaskade + Scoring
   (Tag-Jaccard, optional geblendet mit Last.fm-Similarity, optional
   Popularitäts-Bias), liefert `self.picks`
5. `save_as_playlist()` – optional, Raw-HTTP-Requests (spotipy-Endpunkte
   seit Feb 2026 entfernt)

**Zwei persistente SQLite-Caches** (gleiche Datei, `cache_path`):
- `self._search_db` – **nur Hauptthread**: Spotify-Suchergebnis-Cache
  (`tracks`, `queries`, `query_tracks`)
- `self._lastfm_db` – **cross-thread-sicher** (`check_same_thread=False`,
  über `self._cache_lock` serialisiert): Last.fm-Cache (`lastfm_artists`,
  `lastfm_tracks`)

**Last.fm-Request-Pipeline:** persistenter `requests.Session` (Fix 6) →
`_lastfm_get()` mit Retry/Backoff → `LastFmRequestMonitor` (Fehlerrate/429-Rate/Latenz-Baseline) → adaptive Concurrency in `filter_and_rank`.

---

## 2. Alle Fixes & Features dieser Session

### Last.fm-Requestpfad
| # | Was | Warum | Wo |
|---|---|---|---|
| 1 | Fail-fast-Kaskade in `_fetch_artist_infos_batch`: erst Listener (billigster, ausschlusskräftigster Call), Tags/Top-Tracks nur für Artists, die `min_artist_listeners` bestehen. Beide Pässe flach über den Batch submitted statt pro Artist gebündelt. | Vermeidet 2 von 3 Last.fm-Calls für jeden Artist, der ohnehin gleich rausfliegt. | `_fetch_artist_infos_batch` |
| 2 | `_run_concurrent`: Sliding-Window statt Chunk-Barrier (`wait(..., FIRST_COMPLETED)` statt `as_completed` über ganze Chunks) | Barrier ließ Threads leerlaufen, bis der langsamste im Chunk fertig war | `_run_concurrent` |
| 6 | Persistenter `requests.Session` mit gepooltem `HTTPAdapter` statt `requests.get()` pro Call | `requests.get()` baut pro Call eine neue Session/TCP+TLS-Verbindung auf; Session-Reuse eliminiert das (~1,08s/Call Connect-Overhead gemessen) | `__init__` (`self._lastfm_session`), `_lastfm_get` |
| – | **Persistierter Last.fm-Cache** (SQLite `lastfm_artists`/`lastfm_tracks`, `lastfm_cache_ttl_days`, Migration für Alt-DBs) | In-Memory-Cache war pro Instanz verloren; bei wiederholten/ähnlichen Seeds wurde der dominante Kostenblock (>1800s in einem Fresh-Lauf) jedes Mal neu gemacht | `_load/_save_persisted_artist_field`, `_load/_save_persisted_track_listeners`, alle vier `lastfm_*`-Lookup-Methoden |
| – | 429-Handling: kurzes, begrenztes Retry (`MAX_UNKNOWN_429_RETRIES=2`) für 429 ohne verwertbaren `Retry-After` | spotipy labelt einen transienten 502-Sturm (urllib3 `RetryError`) fälschlich als 429 ohne Header; führte zu Hard-Abort bei rein transienten Spotify-Problemen | `_paginated_search` |

### Kandidatenpool / Diversität / Frische
| # | Was | Warum |
|---|---|---|
| 4 | `"all"` zu `LASTFM_JUNK_TAGS` | Sinnloser, teurer Spotify-Genre-Query, verursachte u.a. den beobachteten 502-Sturm |
| 5 | Snowball-Genre-Expansion (`update_discovered_genres`) verlangt jetzt `_genre_similarity(tags) > 0` bevor ein Artist neue Tags beisteuert | Verhinderte Genre-Drift bei Einzel-Seed-Läufen (z.B. Playboi-Carti-Radio landete bei Fleetwood Mac/Queen) |
| – | **Off-by-N-Fix**: separater `candidate_count`-Zähler statt `len(candidates)` in `build_candidate_pool` | Dict enthielt einen `None`-Platzhalter pro Seed-Track, der mitgezählt wurde → Pool war immer `target_total_candidates - Seedanzahl` groß |
| – | **min_fresh_fraction-Stall-Fix (progressive Cap-Boosts)**: `expansion_multiplier = 1 + expansion_round`, angewendet auf **alle drei** Kandidatenquellen (Genre, Seed-Artist, Similar-Artist) in jeder Expansion-Runde | Bei wiederholten/ähnlichen Seeds war der erreichbare Query-Raum bis zum jeweiligen `max_results_per_*`-Cap bereits gecacht → 0% Fresh Fraction trotz vieler Expansion-Runden, weil jede Query denselben Cap traf. Seed-/Similar-Artist-Quellen liefen vorher nur **einmal** (kein Retry-Mechanismus) |
| – | `capped_at_max_offset`-Tracking (neue Spalte in `queries`, Migration inkl.) | `_max_offset=990` (Spotifys eigenes Offset-Limit) ist eine harte Grenze, die kein Cap-Boost überwinden kann. Wurde vorher nicht als "erschöpft" erkannt → Genre-/Artist-Auswahl verschwendete Boost-Aufwand auf strukturell tote Queries. Jetzt: `_is_query_exhausted` prüft `exhausted OR capped_at_max_offset`; TTL-Reset setzt beides zurück |
| – | `VERSION_SUFFIX_PATTERNS` erweitert um generische, wortgrenzenbasierte Muster (`edit\|mix\|version\|edition\|anniversary\|extended\|demo\|reissue\|expanded\|alternate`) | Alte Liste kannte nur exakte Phrasen ("single version", "radio edit") – "Single Edit", "Special Edition", "Julian Raymond Album Mix" etc. wurden nicht erkannt → bis zu 5 Versionen desselben Songs im Ergebnis. Benannte Remixe (`\bmix\b` matcht nicht "Remix") bleiben bewusst als eigene Einträge erhalten |

### Last.fm-Similarity (Collaborative-Filtering-Signal)
| # | Was | Warum |
|---|---|---|
| – | `lastfm_similar_artists()` (neue, dreistufig gecachte Methode: In-Memory → SQLite → `artist.getSimilar`) | Basis für alles Folgende |
| – | `_ensure_seed_similar_artists()`: 1 Last.fm-Call pro Seed-Artist, idempotent | Speist zwei Dinge: |
| a | Dritte Round-Robin-Kandidatenquelle in `build_candidate_pool` (`self._similar_artist_pool_names`) | Discovery-Signal unabhängig von Genre-Tags |
| b | Scoring-Blend in `filter_and_rank`: `similarity = (1-w)*tag_jaccard + w*lastfm_match`, `w` **einmal pro Lauf** aus `similarity_blend_weight_range` gesampelt (nicht pro Kandidat – konsistent innerhalb eines Laufs, aber Run-zu-Run-Jitter) | Gewünschter zusätzlicher Result-Jitter |
| – | Thin-Tag-Fallback: `thin_tag_genre_threshold`/`thin_tag_similar_artist_count` | Bei tag-armen/regionalen Seeds (z.B. "Ham kummst" von Seiler und Speer) läuft Genre-Suche strukturell leer; Last.fm-Kollaborationsgraph funktioniert unabhängig von Tag-Reichtum |
| – | `popularity_bias`: Perzentil-Rang-basierte Nachbearbeitung (kein zusätzlicher Request), steuert Richtung Mainstream (positiv) oder Hidden-Gems (negativ) | Gegen Hub-Artist-Konvergenz bei populären Genre-Nachbarschaften |

### Sonstiges
| # | Was | Warum |
|---|---|---|
| – | `is_allowed_language`: `_fast_language_hint()` als Vorabprüfung (Skript-Mismatch → hartes `False`; Stopword-Treffer → hartes `True`; sonst `None` → Fallback auf `detect()`) | 40% der Aufrufe in Tests ohne `langdetect` gelöst, ~1000x schneller für die gelöste Teilmenge |
| – | `_CACHE_MISS`-Sentinel: `object()` → `Enum`-Singleton | IDE-Typing-Warning (`__getitem__` nicht auf `object` definiert); `isinstance(x, object)` ist immer `True`, daher als Guard unbrauchbar – Enum wird von Pyright/mypy für `is`-Narrowing erkannt |
| – | `requests.adapters.HTTPAdapter` → explizites `from requests.adapters import HTTPAdapter` | IDE-Warning ("adapters" kein bekanntes Attribut von `requests`) |

---

## 3. Aktuelle Config-Parameter (Referenz)

Alle unten aufgeführten Parameter haben Defaults – bestehende Config-Dateien
ohne diese Keys bleiben lauffähig.

```
# Candidate gathering
max_results_per_similar_artist: int = 15
lastfm_similar_artist_count: int = 8
thin_tag_genre_threshold: int = 4
thin_tag_similar_artist_count: int = 25
similarity_blend_weight_range: tuple = (0.2, 0.5)
popularity_bias: float = 0.0

# Persisted Last.fm cache
lastfm_cache_ttl_days: float = 14

# 429-Handling (Klassenkonstante, nicht Config)
MAX_UNKNOWN_429_RETRIES = 2
```

Empfohlene, aber optionale manuelle Config-Anpassungen aus der Session:
- `lastfm_max_workers`: hoch ansetzen (z.B. 20) – nie throttled beobachtet
- `target_total_candidates`: bei Einzel-Seed-Läufen tendenziell niedriger,
  da der Early-Stop (`oversample_target`) in der Praxis selten greift und
  Poolgröße ≈ Last.fm-Lookup-Anzahl 1:1 ist

---

## 4. Bekannte offene Punkte / Grenzen

- **Spotify-Suche ist nicht parallelisiert** (bewusst zurückgestellt wegen
  Rate-Limit-Risiko). Bei echten Fresh-Queries ca. 40-50% der Walltime.
  Größter verbleibender Geschwindigkeits-Hebel, aber Umbau bräuchte: eigene
  Executor-Instanz, zweite `_search_db`-Connection (cross-thread), robustere
  429-Behandlung unter Nebenläufigkeit.
- **Stark wiederholt getestete Seeds (v.a. "Queen") können strukturell
  "leergefischt" sein**: Wenn der erreichbare Such-String-Raum (Genres +
  Artist-Kataloge) bereits bis `_max_offset=990` oder echter
  Spotify-Erschöpfung durchsucht ist, kann **kein** Cap-Boost mehr helfen –
  nur neue, nie probierte Query-*Strings* (z.B. 2-Hop-Similar-Artists:
  Similar-Artists der Similar-Artists) würden hier noch etwas bringen.
  Nicht umgesetzt, nur diagnostiziert.
- **Konvergenz-Diagnose**: Tag-Jaccard-Scoring belohnt strukturell
  "Hub"-Artists, die an vielen populären Tags gleichzeitig hängen (Beleg:
  "Mrs. Robinson"/"A Horse with No Name" tauchten bei zwei unterschiedlichen
  Cash-Konfigurationen auf). `popularity_bias` und `lastfm_listener_ceiling`
  sind die verfügbaren Gegenhebel – `lastfm_listener_ceiling` steht aktuell
  praktisch deaktiviert in der Nutzer-Config (`1000000000`).
- **Beschreibungsgenerierung**: nur konzeptionell besprochen, nichts
  implementiert (siehe Backlog unten).

---

## 5. Backlog für zukünftige Sessions

### Neu (diese Session, explizit zum Merken)
1. **Porting auf Android** (Form offen – native App, PWA, Termux/Kivy o.ä.
   nicht festgelegt), **eventuell auch Apple/iOS**.
2. **An-/Ausschalt-Mechanismus für "database results"** – insbesondere um
   Nutzung **ohne Spotify Premium** zu ermöglichen. (Noch nicht
   spezifiziert, was genau "database results" hier bedeutet – vermutlich:
   ob Kandidaten/Metadaten aus dem lokalen SQLite-Cache angezeigt werden
   können, auch wenn die Playlist-Erstellung selbst – Web-API-Playback
   braucht Premium – nicht möglich ist. Beim nächsten Anlauf zuerst genauer
   klären, was konkret umgeschaltet werden soll.)
3. **Analytische Description-Generierung.** Hängt vermutlich mit der
   bereits früher besprochenen **prozeduralen** Beschreibungsgenerierung
   zusammen (siehe unten) – beim nächsten Anlauf klären, ob "analytisch"
   dieselbe Idee präzisiert oder einen anderen Ansatz meint (z.B. auf
   Statistik/Feature-Extraktion basierend statt auf Grammatik-Templates).

### Bereits früher besprochen, zurückgestellt
- **Spotify-Suche parallelisieren** (siehe "Bekannte offene Punkte" oben) –
  explizit zurückgestellt wegen Rate-Limit-Sorge.
- **Prozedurale Beschreibungsgenerierung**: Empfehlung war ein
  Tracery-artiger Grammatik-Ansatz (rekursive Ersetzungsregeln) statt
  LLM-Prompting, gespeist aus: dominanten gemeinsamen Tags (`Counter` über
  `info["tags"]` der Picks – aktuell nicht mit im `scored`-Dict persistiert,
  wäre kleine Ergänzung), Ära-Spanne (bräuchte `track["album"]["release_date"]`,
  aktuell nicht abgerufen), Obscurity-Level (aus vorhandenen Listener-Zahlen
  ableitbar), plus einem handgebauten "Vibe-Lexikon" (Last.fm-Tag →
  Adjektiv/Phrase).
- **2-Hop-Similar-Artists** (Similar-Artists der bereits gefundenen
  Similar-Artists) als Ausweg für strukturell leergefischte Seeds – siehe
  "Bekannte offene Punkte".
- **Max-Tracks-pro-Artist-Cap** in der finalen Auswahl – wurde als Idee
  genannt (Beleg: 4 Max-McNown-Tracks in Folge in einem Ergebnis), aber nie
  implementiert.

---

## 6. Hinweis für die nächste Session

Die Datei liegt vollständig, kompiliert und mit allen oben genannten Fixes
vor. main.py wurde nie hochgeladen/gesehen. Für Folgearbeiten am
Last.fm-/Spotify-Pfad: bitte den aktuellen Stand von `SongRadio.py` erneut
hochladen (nicht aus altem Chat-Verlauf rekonstruieren) – spart Tokens und
vermeidet Abschreibfehler bei mittlerweile >2600 Zeilen.
