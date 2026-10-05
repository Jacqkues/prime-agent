# pa-gateway — brique intégrable pour applications agentiques

## Objectif produit

Permettre aux développeurs d'ajouter des agents collaboratifs à leur propre
produit. Le projet hôte conserve son identité, sa base de données, sa facturation,
ses workspaces et son infrastructure. L'adoption dans un projet existant prime
sur la construction d'un SaaS monolithique spécifique à Prime Agent.

Deux modes : bibliothèque Rust et routeur HTTP optionnel, utilisable depuis
JavaScript, Python ou tout client HTTP. Aucun fournisseur d'identité, PostgreSQL,
Docker, port réseau ou schéma de comptes n'est imposé par la bibliothèque.

## Responsabilités

| Composant | Responsabilité |
| --- | --- |
| `pa-agent` | Boucle agentique indépendante des outils |
| `pa-core` | Moteur de session, outils et kernel |
| `pa-daemon` | Workers, supervision, protocole local et file d'exécution |
| `pa-gateway` | Accès multi-utilisateur, sessions partagées, autorisation et API optionnelle |
| Application hôte | Identité vérifiée, stockage durable, quotas, secrets et environnements isolés |

`pa-gateway` dépend de `pa-types` et `pa-telemetry`, sans lier le moteur.
L'adaptateur natif parle le protocole JSONL du daemon. ACP est une interface
JSON-RPC distincte sur stdio ; le socket daemon n'est pas un socket ACP.
Les types partagés sont dans `pa-types::gateway`.

## Collaboration dès le modèle de données

```text
Tenant / organisation
└── Workspace : environnement d'exécution et accès autorisés
    └── Sessions
        └── Membres : propriétaire, contributeurs, lecteurs
```

Un utilisateur peut participer à plusieurs sessions et une session peut avoir
plusieurs utilisateurs. Une session débute privée, avec un propriétaire. Celui-ci
peut inviter un utilisateur déjà autorisé sur le workspace. Seuls les
contributeurs et le propriétaire envoient des messages ; seul le propriétaire
peut inviter, révoquer, arrêter ou fermer. L'auteur est déterminé par le serveur.

Les ACL de session contrôlent l'API. Le code exécuté dans un même workspace
partage son environnement : une ACL de conversation ne remplace pas une sandbox.
L'application doit fournir des environnements distincts pour des clients qui ne
se font pas confiance, ainsi que des secrets et politiques réseau appropriés.

## Première tranche implémentée

- Crate `pa-gateway`, bibliothèque sans feature activée par défaut.
- `Gateway` : création, liste, lecture, partage, révocation, prompts, arrêt,
  fermeture et abonnement aux événements.
- `SessionStore` : adaptation au stockage de l'hôte avec mises à jour atomiques
  par révision ; `MemoryStore` fourni pour développement et tests uniquement.
- `WorkspacePolicy` : accès courant aux workspaces et admission des demandes.
- `Runtime` : adaptation à l'exécution ; `DaemonRuntime` fourni pour des daemons
  isolés et provisionnés par l'hôte.
- Feature `http` : routeur Axum intégrable, authentification remplaçable, JSON
  strict et SSE. Les écritures utilisent HTTP ; WebSocket n'est pas requis.
- Attribution conservée dans les messages utilisateur sous forme JSON ; prompts
  admis dans la file follow-up du daemon.
- Réévaluation des droits sur les flux ouverts et snapshot à la reconnexion.
- Télémétrie d'adoption activée seulement par un client fourni par l'hôte.
- Exemple exécutable et tests de collaboration, transport et intégration HTTP.

Le contrat d'intégration, les routes et leurs limites sont détaillés dans
[le README de la crate](crates/pa-gateway/README.md).

## Persistance et garanties

Le stockage de métadonnées est distinct de l'historique du runtime. La création
passe par `provisioning`, puis `ready` ou `failed`. Une fermeture bloque d'abord
les nouvelles actions via l'API. Une panne entre base et runtime nécessite une
réconciliation par l'hôte ; aucune transaction distribuée n'est simulée.

Une réponse 202 confirme l'admission d'un prompt, pas sa fin. Une déconnexion HTTP
n'annule pas une mutation déjà lancée, mais un arrêt du processus est une autre
frontière. Aucune relance automatique d'un prompt à résultat incertain. Un flux
reconnecté repart d'un snapshot ; il n'existe pas de journal de replay propre à
la gateway. La concurrence des messages est ordonnée par le runtime.

## Suite du produit, à livrer par tranches vérifiables

1. Adaptateur durable de référence et tests contractuels réutilisables pour les
   intégrateurs ; réconciliation explicite des provisions et fermetures incomplètes.
2. Réservation de budget, limites de concurrence et comptabilité des sous-agents
   au niveau de l'exécution, avec une politique remplaçable par l'hôte.
3. Contrat HTTP documenté par OpenAPI, pagination et exemples TypeScript/Python.
4. Routage vers des environnements distants et reprise après remplacement de
   workers ; tests de panne couvrant les frontières base/runtime.
5. Catalogue de modèles autorisés par workspace et permissions d'outils, sans
   introduire une configuration SaaS globale dans `pa-agent`.

## Validation

`make gateway-check` couvre la feature HTTP ; `make check` reste la vérification
workspace ; `make deny` contrôle les dépendances. La CI vérifie aussi HTTP.
Les tests de socket ne remplacent pas la comparaison avec le binaire TS : cette
preuve reste requise avant fusion pour les commandes daemon réutilisées.
