# DuckDB - Pourquoi et comment

## 🤔 Pourquoi DuckDB ?

**Besoin :** Agrégation rapide de milliers de trades/seconde (GROUP BY fenêtre + symbole).

**DuckDB apporte :**
- ✅ **Performance** : Base analytique en mémoire, vecteurisation SIMD, millisecondes pour agrégations
- ✅ **Simplicité** : Bibliothèque Python, pas de serveur, SQL standard
- ✅ **Léger** : Pas de JVM/cluster, démarrage instantané

## 🏗️ Fonctionnement

**Architecture :** Base analytique in-process (dans le processus Python), pas de serveur.

**Cycle :**
1. `conn = duckdb.connect(':memory:')` - Connexion en RAM
2. `CREATE TABLE trades (...)` - Schéma avec types optimisés (BIGINT, DOUBLE, VARCHAR, BOOLEAN)
3. `executemany(INSERT ...)` - Insertion batch (10k+ lignes/seconde)
4. `SELECT ... GROUP BY window_start, symbol` - Agrégation SQL (vecteurisation SIMD)
5. `DELETE FROM trades` - Vidage pour prochain batch

**Fenêtre temporelle :** `FLOOR(trade_time / (WINDOW_SECONDS * 1000)) * (WINDOW_SECONDS * 1000)`

**Métriques calculées :** MAX/MIN/AVG(price), SUM(volume_buy/sell/total), STDDEV(price), COUNT(*)

**Performance :** ~5-10ms pour 1000 trades, ~30-50ms pour 10000 trades

