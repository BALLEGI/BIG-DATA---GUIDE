# 📘 BIG DATA - GUIDE COMPLET POUR L'EXAMEN QCM

## Table des matières
1. **HBASE** — Base orientée colonnes
2. **CASSANDRA** — Base NoSQL distribuée Peer-to-Peer
3. **REDIS** — In-memory Key/Value ultra rapide
4. **NEO4J** — Base orientée graphes
5. **KAFKA** — Streaming Distribué
6. **SPARK** — RDD, DataFrame, Streaming
7. **PIPELINE BIG DATA COMPLET**

---

# 1. HBASE — BASE ORIENTÉE COLONNES

## 1.1 Définition et Contexte

HBase est une base de données NoSQL **orientée colonnes** (Column Family Store) distribuée, construite sur:
- **HDFS** (Hadoop Distributed File System) pour le stockage physique
- **YARN** pour la gestion des ressources
- **ZooKeeper** pour la coordination

Elle est conçue pour:
- **Stocker des milliards de lignes** avec des millions de colonnes
- **Lectures/écritures aléatoires rapides** en temps réel
- **Scalabilité horizontale** (ajouter des nœuds augmente la capacité)

## 1.2 Architecture HBase

```
┌────────────────────────────────────┐
│     Client Applications            │
└─────────────┬──────────────────────┘
              │
     ┌────────┴────────┐
     ▼                 ▼
┌─────────┐      ┌──────────────┐
│ HMaster │      │RegionServers │ (3+)
│         │      │(Stockage)    │
│ Métadat │      │              │
└────┬────┘      └──────────────┘
     │                 ▲
     │                 │
     ▼                 ▼
┌─────────────────────────────┐
│  HDFS (Replication RF=3)    │
│  - HFiles                   │
│  - WAL (Write Ahead Log)    │
└─────────────────────────────┘
     ▲
     │
┌────┴─────────┐
│  ZooKeeper   │ (Leader election)
└──────────────┘
```

### Composants clés:

**HMaster**
- Gère les métadonnées et structure des tables
- Assigne les Regions aux RegionServers
- Gère le load balancing et la répartition
- N'est PAS impliqué dans les opérations de lecture/écriture (pas de bottleneck)

**RegionServer**
- Stocke les données réelles
- Gère les lectures et écritures
- Une machine peut avoir plusieurs RegionServers
- Envoie des heartbeats à HMaster via ZooKeeper

**Region**
- **Découpe horizontale** d'une table (par range de row keys)
- Chaque region est gérée par **UN SEUL** RegionServer à la fois
- Quand une region devient trop grande → **split** automatiquement
- Contient plusieurs Column Families

**ZooKeeper**
- Coordination du cluster (leader election)
- Stocke informations de coordination
- Détecte les pannes via heartbeat monitoring

**HFile**
- Format de stockage physique sur HDFS
- Immuable une fois écrit
- Organisé par Column Family
- Inclut: index, bloom filters, compressionSortie des données

**WAL (Write Ahead Log)**
- Journal d'écriture pour durabilité
- Écrit AVANT d'appliquer les données en mémoire
- Permet la récupération en cas de crash RegionServer

## 1.3 Modèle de Données HBase

HBase utilise un modèle **clé-valeur multidimensionnel**:

```
Table: users
├─ Row Key: user_001
│  ├─ Column Family: info
│  │  ├─ info:name = "Alice" (timestamp: 1000)
│  │  ├─ info:email = "alice@ex.com" (timestamp: 2000)
│  │  └─ (nouvelle version) = "alice.new@ex.com" (ts: 3000)
│  │
│  └─ Column Family: activity
│     ├─ activity:last_login = "2025-01-12" (timestamp: 2500)
│     └─ activity:visits = "1523" (timestamp: 3500)
│
└─ Row Key: user_002
   ├─ Column Family: info
   │  └─ info:name = "Bob" (timestamp: 1001)
   └─ Column Family: activity
      └─ activity:last_login = "2025-01-11"
```

### Concepts fondamentaux:

**1. Row Key (Clé de ligne)**
- Identifiant unique d'une ligne
- **Détermine la partition** (par hash ou range)
- Trié **lexicographiquement**
- Doit être **bien distribuée** pour éviter hotspots
- Exemple: `user_001`, `user_002`

**2. Column Family (Famille de colonnes)**
- **Groupement physique** de colonnes
- Défini lors de la création de la table
- **3-4 families maximum** par table (recommandé)
- Chaque family peut avoir sa propre: compression, TTL, versioning
- Exemple: `info`, `activity`, `security`

**3. Column (Colonne)**
- **Dynamiques** (pas besoin de définir à l'avance)
- Format: `family:qualifier`
- Exemple: `info:name`, `activity:last_login`

**4. Cell (Cellule)**
- Intersection de (Row Key, Column, Timestamp)
- **Chaque cellule a PLUSIEURS versions** (versioning automatique)

**5. Timestamp**
- Par défaut: timestamp système du moment de l'écriture
- Permet le **versioning** automatique
- Anciennes versions peuvent être **purgées** (TTL, MaxVersions)

## 1.4 Architecture de Stockage

**MemStore** (Buffer en mémoire dans RegionServer)
- Accumule les écritures
- Tri automatique par Row Key
- Quand plein → **flush** en HFile sur HDFS

**HFile** (Stockage persistant sur HDFS)
- Immuable (écrit une fois, ne change jamais)
- Comprimé (LZO, Snappy, GZIP, etc.)
- Contient: index, bloom filters pour recherche rapide

**Compaction**
- Fusion de plusieurs HFiles en un plus grand
- Supprime données obsolètes et versions anciennes
- **Minor compaction**: quelques HFiles
- **Major compaction**: tous les HFiles + nettoyage

## 1.5 Commandes HBase Essentielles

### Gestion des tables:
```hbase
create 'users', 'info', 'activity'
list
describe 'users'
disable 'users'
drop 'users'
```

### Opérations de données:
```hbase
# Insertion
put 'users', 'user1', 'info:name', 'Alice'
put 'users', 'user1', 'info:email', 'alice@ex.com'

# Lecture
get 'users', 'user1'
get 'users', 'user1', 'info:name'
get 'users', 'user1', {COLUMN => 'info:email', VERSIONS => 2}

# Scan
scan 'users'
scan 'users', {STARTROW => 'user1', STOPROW => 'user3'}

# Suppression
delete 'users', 'user1', 'info:email'
deleteall 'users', 'user1'
```

### Gestion Column Families:
```hbase
alter 'users', {NAME => 'security'}
alter 'users', {NAME => 'info', TTL => '86400'}
```

## 1.6 Versioning et TTL

**Versioning**
- HBase garde **toutes les versions** par défaut
- Configurable: `alter 'users', {NAME => 'info', VERSIONS => 2}`

**TTL (Time To Live)**
- Suppression automatique après X secondes
- Exemple: `alter 'users', {NAME => 'activity', TTL => '2592000'}` (30 jours)

## 1.7 Points Clés pour l'Examen

| Aspect | Détail |
|--------|--------|
| **Architecture** | HMaster (métadonnées), RegionServer (stockage), ZooKeeper (coordination) |
| **Stockage** | HDFS avec replication factor = 3 |
| **Modèle** | Row Key → Row → Column Families → Columns → Cells (avec versions) |
| **Avantages** | Scalabilité, lectures aléatoires rapides, compression |
| **Inconvénients** | Pas de jointures SQL, pas de transactions |
| **Cas d'usage** | Analytics, logs, séries temporelles, big data |

---

# 2. CASSANDRA — BASE NOSQL PEER-TO-PEER

## 2.1 Définition et Architecture

Cassandra est une base NoSQL **distribuée sans master** (peer-to-peer):
- **Aucun master** → pas de single point of failure
- **Tous les nœuds sont égaux** → vraie décentralisation
- **Optimisée pour écritures massives** (write-heavy)
- Basée sur **eventual consistency** (convergence vers cohérence)

```
┌────────────────────────────────────┐
│    Cluster Cassandra (N nœuds)     │
├────────────────────────────────────┤
│  Node 1    │  Node 2    │  Node 3  │
│ ┌────────┐ │ ┌────────┐ │ ┌──────┐ │
│ │Replica │ │ │Replica │ │ │Data  │ │
│ │  RF=3  │ │ │  RF=3  │ │ │ RF=3 │ │
│ └────────┘ │ └────────┘ │ └──────┘ │
│            │             │          │
│ Token Ring + Consistent Hashing    │
│ (Distribution et Réplication auto) │
└────────────────────────────────────┘
```

## 2.2 Concepts Fondamentaux

**Token Ring**
- Chaque nœud reçoit une plage de tokens (hash)
- Les clés sont hashées et placées selon leur token
- Les N nœuds suivants répliquent les données (RF=N)

**Keyspace**
- Équivalent d'une **base de données** en SQL
- Contient les paramètres de **réplication**
- Exemple: `CREATE KEYSPACE school WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};`

**Table**
- Contient des lignes de données
- Schema flexible (colonnes dynamiques)

**Partition Key**
- **Première clé** dans PRIMARY KEY
- Détermine sur quel nœud les données sont stockées
- **Ne peut pas être NULL**
- Exemple: `student_id` en PRIMARY KEY(student_id, exam)

**Clustering Key**
- **Colonnes suivantes** dans PRIMARY KEY
- Détermine **l'ordre des données** au sein d'une partition
- Permet tri automatique
- Exemple: `exam` en PRIMARY KEY(student_id, exam) → données triées par exam

```sql
CREATE TABLE grades (
  student_id UUID,        -- Partition Key
  exam TEXT,              -- Clustering Key
  score INT,
  PRIMARY KEY (student_id, exam)
);
```

**Replication Factor (RF)**
- Nombre de copies des données
- RF=3 → 3 copies sur 3 nœuds différents
- Tolère pannes de RF-1 nœuds

**Consistency Level (CL)**
- Nombre de nœuds qui doivent répondre pour une opération

| CL | Description | RF=3 |
|----|-------------|------|
| ONE | 1 nœud répond | 1 nœud |
| QUORUM | Majorité réplicas | 2 nœuds |
| ALL | Tous les replicas | 3 nœuds |
| LOCAL_QUORUM | Quorum même datacenter | 2 nœuds |

**Avec RF=3:**
- ONE = 1 nœud (rapide mais risqué)
- QUORUM = 2 nœuds (compromis)
- ALL = 3 nœuds (lent mais sûr)

## 2.3 Commandes CQL (Cassandra Query Language)

### Keyspace:
```sql
CREATE KEYSPACE school 
WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
USE school;
DESCRIBE KEYSPACE school;
```

### Tables:
```sql
CREATE TABLE students (id UUID PRIMARY KEY, name TEXT, age INT);

CREATE TABLE grades (
  student_id UUID,
  exam TEXT,
  score INT,
  PRIMARY KEY (student_id, exam)
);
```

### Insertion:
```sql
INSERT INTO students (id, name, age) VALUES (uuid(), 'Alice', 22);
INSERT INTO students (id, name, age) VALUES (uuid(), 'Bob', 23) USING TTL 60;
```

### Lecture:
```sql
SELECT * FROM students;
SELECT id, name FROM students WHERE id = 12345;
SELECT * FROM grades WHERE student_id = uuid() ORDER BY exam ASC;
```

### Mise à jour:
```sql
UPDATE students SET name = 'Alice New' WHERE id = 12345;
UPDATE students USING TTL 120 SET name = 'Bob' WHERE id = 12345;
```

### Suppression:
```sql
DELETE name FROM students WHERE id = 12345;
DELETE FROM students WHERE id = 12345;
```

## 2.4 Théorème CAP en Cassandra

Cassandra choisit **AP (Availability + Partition Tolerance)**:
- ✅ **Disponibilité** (A): Répond toujours, même avec pannes
- ✅ **Partition Tolerance** (P): Fonctionne avec partitions réseau
- ❌ **Sacrifice Consistency** (C): Peut retourner données obsolètes temporairement

**Eventual Consistency**
- Les données convergent vers un état cohérent avec le temps
- Idéal pour systèmes temps réel où disponibilité est critique

## 2.5 Points Clés pour l'Examen

| Aspect | Détail |
|--------|--------|
| **Architecture** | Peer-to-peer, Token Ring, Consistent Hashing |
| **Pas de master** | Tous les nœuds égaux, réplication automatique |
| **CAP** | AP (Availability + Partition tolerance) |
| **Écritures** | Ultra-rapides (optimisée write-heavy) |
| **Consistency** | Eventual (convergence avec le temps) |
| **Partition Key** | Détermine nœud de stockage |
| **Clustering Key** | Ordre données dans partition |
| **Cas d'usage** | Séries temporelles, logs temps réel, high-write |

---

# 3. REDIS — IN-MEMORY KEY/VALUE ULTRA RAPIDE

## 3.1 Définition et Utilisation

Redis est un **système de caching en mémoire** ultra-rapide:
- **In-memory** (RAM) → latence < 1ms
- **Persistant optionnel** (RDB/AOF)
- **Structures de données riches** (String, List, Hash, Set, Sorted Set, etc.)
- Utilisé pour: caching, sessions, leaderboards, real-time analytics

```
┌──────────────────────┐
│  Application Clients │
└──────────┬───────────┘
           │ Fast Access
           ▼
┌──────────────────────┐
│  Redis (RAM Cache)   │
├──────────────────────┤
│ - Strings            │
│ - Lists              │
│ - Hashes             │
│ - Sets               │
│ - Sorted Sets        │
│ - Bitmaps            │
│ - HyperLogLog        │
└──────────┬───────────┘
           │ Optional Persistence
           ▼
    ┌─────────────┐
    │ RDB / AOF   │ (HDFS/Disque)
    └─────────────┘
```

## 3.2 Types de Données Redis

### 1. String (Chaîne de caractères)
```redis
SET user:1:name "Alice"
GET user:1:name              # "Alice"
SET counter 100
INCR counter                 # 101
APPEND user:1:name " Smith"  # "Alice Smith"
STRLEN user:1:name           # 11
```

**Cas d'usage**: Caching simple, counters, sessions

### 2. List (Liste ordonnée)
```redis
LPUSH messages "msg1"        # Ajouter début
RPUSH messages "msg2"        # Ajouter fin
LLEN messages                # Longueur
LRANGE messages 0 -1         # Récupérer tous
LPOP messages                # Enlever début
RPOP messages                # Enlever fin
```

**Cas d'usage**: Queue, Stack, timeline, notifications

### 3. Hash (Objet avec champs)
```redis
HSET user:1 name "Alice" age "25" email "alice@ex.com"
HGET user:1 name             # "Alice"
HGETALL user:1               # Tous les champs
HMGET user:1 name age        # Plusieurs champs
HEXISTS user:1 name          # Existe? (1/0)
HLEN user:1                  # Nombre champs
HDEL user:1 email            # Supprimer champ
```

**Cas d'usage**: Objets utilisateurs, profils, configurations

### 4. Set (Ensemble sans ordre)
```redis
SADD online_users "user1" "user2" "user3"
SMEMBERS online_users                    # Tous
SISMEMBER online_users "user1"           # Existe? (1/0)
SCARD online_users                       # Taille
SINTER group_a group_b                   # Intersection
SUNION group_a group_b                   # Union
SDIFF group_a group_b                    # Différence
```

**Cas d'usage**: Utilisateurs en ligne, tags, followers, unique visitors

### 5. Sorted Set (Set avec scores)
```redis
ZADD leaderboard 100 "user1" 150 "user2" 120 "user3"
ZRANGE leaderboard 0 -1 WITHSCORES       # Par score croissant
ZREVRANGE leaderboard 0 -1 WITHSCORES    # Par score décroissant
ZSCORE leaderboard "user1"               # Score: 100
ZRANK leaderboard "user1"                # Rang: 0 (premier)
ZINCRBY leaderboard 10 "user1"           # Augmenter score
ZRANGEBYSCORE leaderboard 100 150        # Entre deux scores
```

**Cas d'usage**: Leaderboards, rankings, séries temporelles

### 6. Bitmap & HyperLogLog
```redis
# BITMAP - compter visiteurs uniques
SETBIT visitors:2025-01-01 0 1           # User 0 visité
SETBIT visitors:2025-01-01 5 1           # User 5 visité
BITCOUNT visitors:2025-01-01             # 2 visiteurs

# HYPERLOGLOG - estimer cardinality
PFADD unique_visitors "user1" "user2" "user1"
PFCOUNT unique_visitors                  # ~2
```

## 3.3 Commandes Clés Redis

```redis
# Gestion clés
EXISTS user:1                # Existe? (1/0)
DEL user:1 user:2           # Supprimer
TYPE user:1                 # string/list/hash/set/zset
EXPIRE user:1 3600          # Expirer dans 1h
TTL user:1                  # Temps restant (secondes)
RENAME old_key new_key      # Renommer

# Transactions
MULTI                       # Début transaction
SET user:1 "Alice"
INCR counter
EXEC                        # Exécuter tous

# Database
FLUSHDB                     # Vider DB courante
FLUSHALL                    # Vider toutes les DBs
```

## 3.4 Persistence Redis

**RDB (Redis Database)**
- Snapshot point-in-time
- Configuration: `SAVE 60 1000` (save si 1000 keys en 60s)
- Commande: `BGSAVE`
- Avantage: Compact, rapide chargement

**AOF (Append Only File)**
- Enregistre TOUTES les commandes
- Plus lent mais plus sûr
- Configuration: `appendonly yes`
- Avantage: Durable, reconstructible

**Sans Persistence**
- Redis perd tout en crash
- Acceptable pour cache/sessions

## 3.5 Points Clés pour l'Examen

| Aspect | Détail |
|--------|--------|
| **In-Memory** | Ultra-rapide (< 1ms) |
| **Structures** | String, List, Hash, Set, Sorted Set, Bitmap, HyperLogLog |
| **Persistence** | RDB ou AOF (optionnel) |
| **Cas d'usage** | Caching, sessions, leaderboards, real-time |
| **Avantages** | Très rapide, structures riches, pub/sub |
| **Inconvénients** | Limité RAM, pas HA native (besoin Cluster) |
| **Pas idéal** | Données persistantes massives, transactions complexes |

---

# 4. NEO4J — BASE ORIENTÉE GRAPHES

## 4.1 Définition et Concepts

Neo4j est une base de données spécialisée pour les **graphes** (réseaux):
- **Nœuds** (entités) et **Relations** (liens)
- Traversals extrêmement rapides
- Optimal pour requêtes relationnelles complexes

```
         ┌──────────────┐
         │     User     │ (Node)
         │ Label: User  │
         │ {id:1,      │
         │  name:Alice} │ (Properties)
         └──────┬───────┘
                │
           FOLLOWS (Relationship)
                │
                ▼
         ┌──────────────┐
         │     User     │
         │ {id:2,      │
         │  name:Bob}   │
         └──────┬───────┘
                │
              LIKES
                │
                ▼
         ┌──────────────┐
         │    Post      │
         │ {id:100,    │
         │  title:...}  │
         └──────────────┘
```

## 4.2 Terminologie Neo4j

**Node (Nœud)**
- Entité ou objet
- Avec **Labels** (catégories): User, Post, Product
- Avec **Properties** (attributs): {name, age, email}

**Relationship (Relation)**
- Lien **orienté** entre deux nœuds
- Avec **Type**: FOLLOWS, LIKES, CREATED, WORKS_FOR
- Peut avoir **Properties**: {weight: 5, since: 2020}

**Label**
- Catégorie de nœud
- Un nœud peut avoir plusieurs: `User:Admin:Premium`

**Property**
- Attribut clé-valeur d'un nœud ou relation

## 4.3 Cypher - Langage de Requête

### Créer:
```cypher
CREATE (u:User {name:'Alice', age:25})
CREATE (u:User {name:'Bob'}), (p:Post {title:'Hello'})
CREATE (u:User {name:'Alice'})-[:FOLLOWS]->(b:User {name:'Bob'})
CREATE (u)-[r:FOLLOWS {since:2020}]->(b)
```

### Matcher (Rechercher):
```cypher
MATCH (u:User) RETURN u
MATCH (u:User {name:'Alice'}) RETURN u
MATCH (u:User {name:'Alice'})<-[:FOLLOWS]-(follower) RETURN follower
MATCH (u:User {name:'Alice'})-[:FOLLOWS]->(following) RETURN following
```

### Amis d'amis:
```cypher
-- Alice -> Bob -> Charlie
MATCH (u:User {name:'Alice'})-[:FOLLOWS]->()-[:FOLLOWS]->(fof) 
RETURN DISTINCT fof
```

### Modification:
```cypher
MATCH (u:User {name:'Alice'}) SET u.age = 26 RETURN u
MATCH (u:User {name:'Alice'}) SET u:Admin
MATCH (a:User {name:'Alice'}), (b:User {name:'Bob'})
CREATE (a)-[:FOLLOWS]->(b)
```

### Suppression:
```cypher
MATCH (u:User {name:'Alice'}) DELETE u
```

### Agrégations:
```cypher
MATCH (u:User)-[:FOLLOWS]->(following) RETURN count(following)
MATCH (u:User)-[:LIKES]->(p:Post) 
RETURN u.name, count(p) as likes_count
RETURN u.name, count(p) as likes ORDER BY likes DESC LIMIT 10
```

## 4.4 Cas Pratiques Neo4j

**Recommandations: Posts aimés par amis**
```cypher
MATCH (me:User {name:'Alice'})-[:FOLLOWS]-(friend)-[:LIKES]->(post)
RETURN DISTINCT post
```

**Chemin le plus court**
```cypher
MATCH p = shortestPath(
  (u1:User {name:'Alice'})-[*]-(u2:User {name:'Charlie'})
)
RETURN p
```

**Utilisateurs à distance 2**
```cypher
MATCH (alice:User {name:'Alice'})-[*2..2]-(others)
RETURN others
```

## 4.5 Points Clés pour l'Examen

| Aspect | Détail |
|--------|--------|
| **Structure** | Nœuds + Relations + Properties |
| **Langage** | Cypher |
| **Forces** | Requêtes relationnelles rapides, graphes |
| **Faiblesse** | Pas optimal données plates, agrégations complexes |
| **Cas d'usage** | Réseaux sociaux, recommandations, knowledge graphs |
| **Scalabilité** | Bonne pour graphes, moins pour tables énormes |

---

# 5. KAFKA — STREAMING DISTRIBUÉ

## 5.1 Définition et Architecture

Kafka est une **plateforme de streaming** distribuée pour:
- **Ingestion temps réel** (millions msg/sec)
- **Découplage** entre producteurs et consommateurs
- **Réplication et durabilité** automatiques
- **Rejeu** de messages possible

```
┌─────────────────────────────────┐
│    Producer Applications        │
│ (Web servers, IoT, logs)        │
└──────────────┬──────────────────┘
               │ PUBLISH
               ▼
┌─────────────────────────────────┐
│   Topic: "logs" (3 partitions)  │
├─────────────────────────────────┤
│ Part 0 │ Part 1 │ Part 2        │
│ [----] │ [----] │ [----]        │
│ (RF=2) │ (RF=2) │ (RF=2)        │
└──────────────┬──────────────────┘
               │
    ┌──────────┼──────────┐
    ▼          ▼          ▼
 Broker0    Broker1    Broker2
   (L)       (R)       (R)
    ▲          ▲          ▲
    └──────────┼──────────┘
               │
         ┌─────┴─────┐
         │ ZooKeeper │
         └───────────┘
               ▲
    ┌──────────┼──────────┐
    ▼          ▼          ▼
┌──────────────────────────────────┐
│  Consumer Group (Spark/App)      │
│  C0 → P0 │ C1 → P1 │ C2 → P2    │
└──────────────────────────────────┘
```

## 5.2 Concepts Fondamentaux

**Topic**
- **Flux logique** de messages
- Immuable une fois écrit
- Exemple: "logs", "events", "user_actions"

**Partition**
- Subdivision d'un topic pour **parallélisation**
- Chaque message → **UNE** partition
- Les partitions sont **ordonnées** (FIFO par partition)
- Réplication: chaque partition sur plusieurs brokers

**Offset**
- Position d'un message dans une partition
- Commence à 0
- Monotone (toujours croissant)
- Permet **rejeu** à partir d'un point

**Producer**
- Envoie messages
- Peut spécifier **clé** (détermine partition)
- Ou Kafka décide (round-robin)

**Consumer**
- Lit messages
- Connaît son offset courant
- Peut se réinitialiser à ancien offset

**Consumer Group**
- Ensemble de consumers lisant le même topic
- Chaque partition lue par **UN SEUL consumer du group**
- Permet parallélisme et failover

**Broker**
- Serveur Kafka
- Gère les partitions
- Un broker = **leader**, autres = **replicas**

**Replication**
- Chaque partition replicée sur plusieurs brokers
- Durabilité: si broker tombe, replicas prennent relais

## 5.3 Commandes Kafka

### Topics:
```bash
kafka-topics --create --topic logs --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 2
kafka-topics --list --bootstrap-server localhost:9092
kafka-topics --describe --topic logs --bootstrap-server localhost:9092
```

### Producer:
```bash
kafka-console-producer --topic logs --bootstrap-server localhost:9092
# Taper messages puis Enter
```

### Consumer:
```bash
kafka-console-consumer --topic logs --bootstrap-server localhost:9092 \
  --from-beginning
kafka-console-consumer --topic logs --bootstrap-server localhost:9092 \
  --group my_app --from-beginning
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group my_app --describe
```

## 5.4 Garanties Kafka

**Order**
- ✅ Garanti **par partition**
- ❌ Non garanti **globalement** (multi-partitions)

**Durabilité**
- `acks=all`: attend tous les replicas (lent, sûr)
- `acks=1`: seulement leader (moyen)
- `acks=0`: fire-and-forget (rapide, risqué)

**Livraison**
- **At-least-once**: peut traiter 2x (idempotence nécessaire)
- **Exactly-once**: mode transactionnel (Kafka 0.11+)

## 5.5 Points Clés pour l'Examen

| Aspect | Détail |
|--------|--------|
| **Haute débit** | Millions msg/sec |
| **Partition** | Parallélisation + ordre (par partition) |
| **Offset** | Position message, permet rejeu |
| **Consumer Group** | 1 consumer per partition |
| **Replication** | Durabilité automatique |
| **Cas d'usage** | Logs, events, stream processing |
| **Avantage** | Durable, replay, découplage |
| **Désavantage** | Plus complexe qu'une queue simple |

---

# 6. SPARK — RDD, DATAFRAME, STREAMING

## 6.1 Qu'est-ce que Spark?

Spark est un framework de **traitement distribué**:
- **In-memory** processing (100x plus rapide que MapReduce)
- Trois niveaux: **RDD** (bas niveau), **DataFrame** (optimisé), **Streaming**
- Langages: Python, Scala, Java, SQL

## 6.2 RDD (Resilient Distributed Dataset)

**RDD** = structure de données distribuée **immuable** et **résiliente**

### Création:
```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd = sc.textFile("data.txt")
new_rdd = rdd.map(lambda x: x * 2)
```

### Transformations (Lazy - Ne s'exécutent PAS immédiatement):
```python
# MAP - appliquer fonction à chaque élément
rdd2 = rdd.map(lambda x: x * 2)            # [2, 4, 6]

# FLATMAP - map + flatten
words = words_rdd.flatMap(lambda line: line.split(" "))

# FILTER - garder éléments satisfaisant condition
even = rdd.filter(lambda x: x % 2 == 0)   # [2, 4]

# REDUCEBYKEY - regrouper par clé et réduire
rdd = sc.parallelize([("A", 1), ("B", 1), ("A", 1)])
counts = rdd.reduceByKey(lambda x, y: x + y)
# [("A", 2), ("B", 1)]

# JOIN - combiner deux RDDs
rdd1 = sc.parallelize([("Alice", 25), ("Bob", 30)])
rdd2 = sc.parallelize([("Alice", "Eng"), ("Bob", "Mgr")])
joined = rdd1.join(rdd2)
# [("Alice", (25, "Eng")), ("Bob", (30, "Mgr"))]

# UNION - combiner sans grouping
union = rdd1.union(rdd2)
```

### Actions (Eager - Exécution immédiate):
```python
result = rdd.collect()              # Ramène tout (⚠️ OOM risk!)
count = rdd.count()                 # Nombre éléments
first_two = rdd.take(2)             # 2 premiers
first = rdd.first()                 # 1er élément
rdd.saveAsTextFile("/tmp/output")   # Écrire fichiers
rdd.foreach(lambda x: print(x))     # Appliquer fonction
```

## 6.3 DataFrame

**DataFrame** = RDD avec structure (colonnes nommées, types)

### Création:
```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df = spark.read.json("data.json")
df = spark.read.parquet("data.parquet")
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
```

### Opérations:
```python
df.select("name", "age").show()
df.filter(df.age > 25).show()
df.groupBy("city").count().show()
df.orderBy("age", ascending=False).show()
df.limit(10).show()
df.dropna().show()
```

### SQL sur DataFrame:
```python
df.createOrReplaceTempView("users")
result = spark.sql("SELECT name, age FROM users WHERE age > 25")
result.show()

# Agrégations
result = spark.sql("""
  SELECT city, COUNT(*) as count, AVG(salary) as avg_salary
  FROM users
  GROUP BY city
  ORDER BY count DESC
""")
```

## 6.4 Spark Streaming

### Structured Streaming (Recommandé):
```python
# Lire Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs") \
    .load()

# Transformer
parsed = df.select(explode(split(col("value"), " ")).alias("word"))

# Grouper
counts = parsed.groupBy("word").count()

# Écrire résultat
query = counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

# Modes: complete (tout), update (changements), append (nouveaux)
```

## 6.5 DAG et Lazy Evaluation

**DAG (Directed Acyclic Graph)**
- Représentation graphique du plan d'exécution
- Chaque nœud = transformation/action
- Edges = dépendances

**Lazy Evaluation**
- Transformations n'exécutent PAS immédiatement
- Seules **Actions** déclenchent exécution
- Permet **optimisation globale** (Catalyst)

```python
rdd = sc.parallelize([1, 2, 3])
rdd2 = rdd.map(lambda x: x * 2)      # Pas exécuté!
rdd3 = rdd2.filter(lambda x: x > 3)  # Pas exécuté!
result = rdd3.collect()               # MAINTENANT exécuté!
```

## 6.6 Narrow vs Wide Dependencies

**Narrow Dependency**
- Chaque partition parent utilisée par **UN SEUL** enfant
- Pas de shuffle
- Exemple: map, filter, flatMap
- Pipeline execution

**Wide Dependency**
- Chaque partition parent peut être utilisée par **PLUSIEURS** enfants
- **Nécessite shuffle** (regroupement entre nœuds)
- Exemple: reduceByKey, join, groupByKey
- Crée stage boundary

```python
rdd = sc.parallelize([1, 2, 3, 4])

# Narrow (une stage)
rdd1 = rdd.map(lambda x: x * 2)
rdd2 = rdd1.filter(lambda x: x > 3)

# Wide (deux stages)
rdd3 = rdd.map(lambda x: (x % 2, x))
rdd4 = rdd3.reduceByKey(lambda x, y: x + y)  # Shuffle!
```

## 6.7 Points Clés pour l'Examen

| Aspect | Détail |
|--------|--------|
| **RDD** | Bas niveau, flexible, immuable |
| **DataFrame** | Optimisé, colonnes nommées, SQL |
| **Lazy** | Transformations pas exécutées immédiatement |
| **Actions** | Seules things qui déclenchent exécution |
| **DAG** | Plan optimisé d'exécution |
| **Narrow** | Map, filter → pas shuffle |
| **Wide** | reduceByKey, join → shuffle |
| **Streaming** | Micro-batches ou event-based |
| **Avantage** | 100x rapide MapReduce, flexible |

---

# 7. PIPELINE BIG DATA COMPLET

## 7.1 Architecture End-to-End

```
┌────────────────────────────────┐
│  Real-time Data Sources        │
│ (IoT, Web servers, Logs)       │
└──────────────┬─────────────────┘
               │
               ▼
        ┌─────────────┐
        │   KAFKA     │ (Ingestion)
        │             │
        │ - Topics    │
        │ - Partitions│
        │ - Replication
        └──────┬──────┘
               │
               ▼
     ┌──────────────────┐
     │ SPARK STREAMING  │ (Processing)
     │                  │
     │ Parse + Transform│
     │ Real-time aggr   │
     └────────┬─────────┘
              │
     ┌────────┴────────┐
     │                 │
     ▼                 ▼
  ┌──────┐       ┌──────────┐
  │REDIS │       │  HBASE   │
  │      │       │          │
  │Cache │       │ Analytics│
  │Fast  │       │ Durable  │
  └──────┘       └──────────┘
     ▲                 ▲
     │                 │
  ┌──────────────────────┐
  │   Application Layer  │
  │  (Serve results)     │
  └──────────────────────┘
```

## 7.2 Flux Complet: Real-time Log Analytics

```python
# 1. KAFKA - Producer envoie logs
# Format: {"timestamp": 1000, "level": "ERROR", "service": "api"}

# 2. SPARK STREAMING - Lit Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs") \
    .load()

# 3. Parse et transforme
from pyspark.sql.functions import from_json, col

schema = "timestamp LONG, level STRING, service STRING"
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 4. Agrégation temps-réel (Erreurs par service)
errors_per_service = parsed \
    .filter(col("level") == "ERROR") \
    .groupBy("service") \
    .count()

# 5. Écrire résultat en KAFKA (pour alerting)
query = errors_per_service \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# 6. (Optionnel) Consommer "alerts" et stocker en HBase/Redis
# - Données hot/rapides → Redis (cache)
# - Données froides/longues → HBase (analytics)
```

## 7.3 Choix par Cas d'Usage

| Composant | Quand utiliser | Avantages |
|-----------|----------------|-----------|
| **HBase** | Stockage long terme, analytics | Scalable, durable, ordered |
| **Cassandra** | Séries temporelles, high-write | Disponibilité, ultra-rapide écriture |
| **Redis** | Cache, sessions, real-time | Très rapide, structures riches |
| **Neo4j** | Graphes, recommandations | Traversals rapides |
| **Kafka** | Streaming, événements | Durable, replay, découplage |
| **Spark** | Batch et stream processing | Flexible, rapide, 100x MapReduce |

## 7.4 Exemple Complet Multi-Source

**Scenario:** Analyser interactions utilisateurs temps réel

```
┌─────────────────────────────────┐
│ Multiple Data Sources           │
├─────────────────────────────────┤
│ - Web logs (Kafka)              │
│ - User DB (HBase)               │
│ - Cache (Redis)                 │
│ - Graph (Neo4j): followers      │
└──────────┬──────────────────────┘
           │
           ▼
    ┌────────────────┐
    │ Spark Pipeline │
    ├────────────────┤
    │ 1. Lire logs   │
    │ 2. Parse JSON  │
    │ 3. Join HBase  │
    │ 4. Neo4j query │
    │ 5. Redis cache │
    └────────┬───────┘
             │
        ┌────┴─────┐
        ▼          ▼
    Alertes    Dashboard
    (Topics)   (App)
```

## 7.5 Points Clés Pipeline pour l'Examen

| Aspect | Détail |
|--------|--------|
| **Ingestion** | Kafka (durable, rejou) |
| **Processing** | Spark (in-memory, flexible) |
| **Storage** | HBase (analytics), Cassandra (TS), Redis (cache) |
| **Queries** | Neo4j (graphes), SQL (HBase/Spark) |
| **Real-time** | Redis cache pour données hot |
| **Durable** | HBase pour données froides |
| **Order** | Kafka garantit par partition |
| **Scalability** | Ajout partitions/brokers/nœuds |

---

# 📋 RÉSUMÉ COMPARATIF FINAL

## Tableau Synthétique

| Technologie | Type | Architecture | CAP | Cas d'usage |
|-------------|------|--------------|-----|------------|
| **HBase** | Column Store | Master-slave (HDFS) | CP | Analytics, big data |
| **Cassandra** | NoSQL | Peer-to-peer | AP | Séries tempo, high-write |
| **Redis** | Cache/KV | In-memory | CA | Sessions, leaderboards |
| **Neo4j** | Graphe | Embedded graph | CA | Réseaux sociaux |
| **Kafka** | Stream | Distributed | AP | Ingestion events |
| **Spark** | Processing | Distributed | - | Batch + stream |

## Critères de Choix

**Besoin scalabilité + durabilité?** → **HBase**
**Besoin ultra-rapide écriture?** → **Cassandra**
**Besoin caching très rapide?** → **Redis**
**Besoin relations complexes?** → **Neo4j**
**Besoin ingestion events?** → **Kafka**
**Besoin processing flexible?** → **Spark**

## Points Critiques à Mémoriser

✓ **HBase**: Column Families, Regions, WAL, MemStore→HFile  
✓ **Cassandra**: Token Ring, RF, CL, AP (eventual consistency)  
✓ **Redis**: Structures (String/List/Hash/Set/ZSET), persistence optionnelle  
✓ **Neo4j**: Nœuds, Relations, Labels, Cypher language  
✓ **Kafka**: Topics, Partitions, Offsets, Consumer Groups, order garantie par partition  
✓ **Spark**: RDD lazy, transformations/actions, DAG, narrow/wide dependencies  
✓ **Pipeline**: Kafka → Spark → (Redis/HBase/Cassandra)

---

Conseils d'étude:
- Mémoriser les **commandes essentielles** pour chaque technologie
- Comprendre les **architectures** (pas juste les commandes)
- Connaître les **cas d'usage** et **trade-offs** de chaque choix
- Dessiner les **diagrammes** mentalement pour les comprendre
- Pratiquer avec les **exemples concrets** du cours
