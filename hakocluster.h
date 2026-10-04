#ifndef HAKOCLUSTER_H
#define HAKOCLUSTER_H

/* hakocluster C ABI (go/pascal consumers). Strings are NUL-terminated
 * UTF-8. Every returned char* is heap-owned: free with
 * hk_cluster_string_free. Errors surface via hk_cluster_last_error;
 * data calls return NULL on error (get also on plain key-miss).
 *
 * Query JSON: {"collection":"b","where":{"field":"g","op":"eq",
 * "value":"g1"},"limit":20} ("where" optional; "eq" only for now).
 * Config JSON: {"durability":"interval","interval_ms":5,
 * "sock_dir":"...","max_lag":5000000,"stagger":"a",
 * "stagger_offset_ms":1} — all keys optional; "stagger":"b" takes
 * "intervals":[5,7], "c" selects ManualRotation (which additionally
 * requires "durability":"manual").
 */

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct HK_Cluster HK_Cluster;

/* Open: paths_json = '["/data/a","/data/b"]'. Convenience overload with
 * default config + explicit sock dir (NULL sock_dir = ./socks). */
HK_Cluster *hk_cluster_open(const char *paths_json, const char *sock_dir);

/* Full-config open (stagger policies, ManualRotation, lag guard). */
HK_Cluster *hk_cluster_open_with_config(const char *paths_json,
                                        const char *config_json);

/* Close (also stops socket tasks on unix). */
void hk_cluster_close(HK_Cluster *handle);

/* Write a doc given as JSON object; returns the id. Engine stamps _time. */
char *hk_cluster_put(HK_Cluster *handle, const char *collection,
                     const char *doc_id, const char *doc_json);

/* Point read as JSON (with "_time"). NULL on miss or error. */
char *hk_cluster_get(HK_Cluster *handle, const char *collection,
                     const char *doc_id);

/* Delete through the designated writer. 0 ok, -1 error. */
int hk_cluster_delete(HK_Cluster *handle, const char *collection,
                      const char *doc_id);

/* Fan-out query. Returns '[{"id":"..","doc":{..}},...]'. */
char *hk_cluster_query(HK_Cluster *handle, const char *query_json);

/* Manual failover. Returns the promotion epoch, -1 on error. */
long long hk_cluster_promote(HK_Cluster *handle, size_t index);

/* Current promotion epoch. */
unsigned long long hk_cluster_epoch(HK_Cluster *handle);

/* Apply the lag guard + report health as JSON:
 * '[{"index":0,"lag":0,"healthy":true},...]'. */
char *hk_cluster_refresh_health(HK_Cluster *handle);

/* Option C rotation tick. 0 ok, -1 error. */
int hk_cluster_tick_flush(HK_Cluster *handle);

/* Multidatabase registry (one process, N named databases).
 * Config JSON: '{"sock_root":"...","databases":[{"name":"billing",
 * "paths":["/data/b1"],"config":{...}}]}' — per-db "config" reuses the
 * shape above (optional); "sock_root" required (each database meshes
 * under sock_root/{name}: the mesh boundary). Fixed at open; unknown
 * names are always an error, never a default. */
typedef struct HK_Databases HK_Databases;

HK_Databases *hk_databases_open(const char *config_json);
void hk_databases_close(HK_Databases *handle);

/* Exact-name lookup: fresh HK_Cluster box over the SAME cluster (one
 * mesh, many handles — close with hk_cluster_close). NULL on unknown. */
HK_Cluster *hk_db_get(HK_Databases *handle, const char *name);

/* Declared names in declaration order, as a JSON array string. */
char *hk_databases_names(HK_Databases *handle);

/* Free a string returned by any hk_cluster_* call. */
void hk_cluster_string_free(char *value);

/* Last error text (borrowed; do not free). NULL when the last call
 * succeeded. */
const char *hk_cluster_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* HAKOCLUSTER_H */
