# Projet FORGE — Supervision d'usine

*Framework imposé : **Quarkus 3.19 + Kafka Streams***

## Contexte

Vous rejoignez l'équipe Data Industrie de **FORGE**, la plateforme de
supervision d'un groupe industriel (pensez Siemens MindSphere). Soixante
machines équipent cinq ateliers. Chacune envoie un **relevé capteur** toutes
les quinze secondes — température, vibration, régime — et déclare chaque
**pièce produite**, bonne ou au rebut.

Quatre enjeux métier. La **dérive d'outil** d'abord : un outil qui s'use fait
grimper le taux de rebut, et plus tôt on le voit, moins on jette. Le **top
produits par atelier** ensuite, que la planification réclame. La **valeur
produite** surtout, parce que c'est ce qui alimente le calcul de rendement —
et une pièce au rebut ne vaut rien. Et enfin les **machines muettes**.

Ce dernier point est le cœur du sujet. Quand une passerelle tombe, la machine
**continue de produire** : les pièces arrivent, tout a l'air normal. Seuls les
relevés s'arrêtent. On pilote alors une machine à l'aveugle, parfois pendant
des heures, sans que rien ne clignote nulle part. **Le silence est
l'information.**

Les automates envoient des messages cassés : champs manquants, types
invalides, JSON tronqué, doublons, retardataires. **Le flux ne sera jamais
propre : c'est à votre pipeline d'être robuste.**

## Mission

Construire une application **Quarkus + Kafka Streams** qui consomme les relevés
et la production, écarte proprement les messages invalides, et produit
détections de dérive, tops par atelier, valeur produite et alertes de machine
muette.

## Architecture

```
forge.readings ──────┐
                     ├──> [ VALIDATION ] ──> invalides ──> <grp>.forge.dlq
forge.production ────┘          │
                                ▼ valides
       ┌───────────────┬────────┴──────┬─────────────────┐
       ▼               ▼               ▼                 ▼
  dérive d'outil   top produits     valeur          machine muette
   (tumbling       (hopping 15/5   (cumul par      (aucun relevé
    10 min)         + jointure)     machine)        depuis 8 min)
       │               ▲               │                 │
       ▼               │               ▼                 ▼
 <grp>.forge.    forge.machines   <grp>.forge.     <grp>.forge.
 scrap           (GlobalKTable)   value            alerts.silent
                       │
                       ▼
              <grp>.forge.top.by-atelier
```

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`forge.readings`** — clé : `machine_id` — un relevé toutes les ~15 s par
machine, **régulièrement** :

```json
{
  "reading_id": "rd-3b8faa18",
  "machine_id": "mac-042",
  "temperature_c": 61.4,
  "vibration_mm_s": 1.87,
  "rpm": 2140,
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

**`forge.production`** — clé : `machine_id` — une pièce déclarée :

```json
{
  "unit_id": "u-9a1f0c33",
  "machine_id": "mac-042",
  "product_id": "ref-017",
  "scrap": false,
  "cycle_time_ms": 18400,
  "unit_value_eur": 84.30,
  "timestamp": "2026-07-04T14:03:49.104Z"
}
```

**`forge.machines`** — topic **compacté**, clé : `machine_id` — la fiche
machine. C'est la seule source de l'`atelier` :

```json
{
  "machine_id": "mac-042",
  "label": "HX-400 17",
  "atelier": "USINAGE",
  "ligne": "USI-L2",
  "modele": "TR-120"
}
```

`atelier` ∈ `EMBOUTISSAGE | USINAGE | ASSEMBLAGE | PEINTURE | CONTROLE`.

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                                | Impact si non gérée                                                          |
|----------------------------------------|----------------------------------------|------------------------------------------------------------------------------|
| Champ requis absent                    | pas de `machine_id`                    | pièce non attribuable                                                        |
| Champ à `null`                         | `"product_id": null`                   | NullPointerException                                                         |
| Mauvais type                           | `"rpm": "douze"`                       | crash de désérialisation                                                     |
| Mauvais type piégeux                   | `"rpm": true`                          | en JSON, `true` n'est pas un nombre                                          |
| Valeur hors bornes                     | `unit_value_eur: -9999`                | valeur produite fausse                                                       |
| Timestamp illisible                    | `"hier a 15h"`                         | fenêtrage cassé                                                              |
| JSON tronqué / non-JSON / message vide | `{"unit_id": "u`                       | **poison pill : l'appli meurt en boucle**                                    |
| Événement en retard                    | timestamp − 30 à 180 min               | **un relevé vieux de 90 min ne prouve pas qu'une machine parle aujourd'hui** |
| Doublon exact                          | même `unit_id` deux fois               | pièce comptée deux fois                                                      |
| Machine hors catalogue                 | `new-0002` absente de `forge.machines` | pièces perdues si `join` au lieu de `leftJoin`                               |

### Sorties (à produire, préfixées par votre groupe)

`<grp>.forge.dlq` · `<grp>.forge.scrap` · `<grp>.forge.top.by-atelier` ·
`<grp>.forge.value` · `<grp>.forge.alerts.silent` — constantes prêtes dans
`Topics.java`. Format des valeurs libre **mais en JSON documenté dans votre
README de rendu**.

## Backlog

> La base 8/20 = FOR-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**FOR-1 — Ingestion fiable (socle, obligatoire)**
Consommer `forge.readings` **et** `forge.production`, parser, valider (champs
requis, grandeurs numériques dans les bornes, `scrap` booléen, timestamp
ISO-8601 parsable). Invalides → DLQ **avec message original + raison**.
L'application ne doit **jamais** crasher.
*Critères : 10 min sans crash ; DLQ motivée ; aucun message valide perdu.*

**FOR-2 — Dérive d'outil** → `<grp>.forge.scrap`
Sur **fenêtres tumbling de 10 min** : `rebuts / pièces` par `machine_id`.
Marquer les dérives : taux > 15 % **avec au moins 50 pièces**. Ici le garde-fou
sert vraiment : la plupart des machines produisent trop peu pour que le ratio
veuille dire quoi que ce soit. Le générateur lance une dérive toutes les
~5 min.
*Critères : chaque `derive_outil` ressort marquée ; une machine à 4 pièces dont
2 rebuts n'est PAS marquée en dérive.*

**FOR-3 — Top produits par atelier** → `<grp>.forge.top.by-atelier`
Compter les pièces par (`atelier`, `product_id`) sur **fenêtres hopping 15 min
/ avance 5 min**. L'`atelier` n'est pas dans l'événement : il vient de la fiche
machine via **GlobalKTable** sur `forge.machines` — il faut donc enrichir
**avant** de grouper.
*Critères : sortie portant atelier + produit ; les machines absentes du
catalogue ressortent en atelier `INCONNU` au lieu de disparaître ; choix
GlobalKTable vs KTable justifié à l'oral.*

**FOR-4 — Valeur produite par machine** → `<grp>.forge.value`
Cumuler `unit_value_eur` par `machine_id` (KTable, `aggregate`). **Une pièce au
rebut ne vaut rien.**
*Critères : montants cohérents ; un doublon d'`unit_id` ne compte pas deux fois
(dédoublonnage à justifier) ; une pièce rejetée en DLQ n'est pas comptée.*

**FOR-5 — Machine muette** → `<grp>.forge.alerts.silent`
Signaler toute machine dont le dernier relevé remonte à plus de
`SILENCE_MINUTES` (déjà câblé dans `TopologyProducer` — gardez-le modifiable
sans recompiler). Chaque machine émet toutes les ~15 s : un trou de plusieurs
minutes n'arrive jamais tout seul. Le générateur coupe la passerelle de trois
machines toutes les ~20 min, pendant 15 min.

**Lisez ce paragraphe deux fois.** Aucune fenêtre du DSL n'émet quoi que ce
soit sur une fenêtre **vide** : compter les relevés et guetter un zéro ne
produira jamais rien, jamais. Il vous faut une horloge qui vienne vous
réveiller pour aller inspecter un état — ce que le DSL seul ne sait pas faire.
Et souvenez-vous que la machine **continue de produire** pendant la panne : ce
n'est pas l'absence de *tout* événement que vous traquez, c'est l'absence de
*relevé*.

*Critères : chaque `panne_passerelle` est détectée avec sa machine ; silence en
régime normal ; **un relevé vieux de 90 min qui arrive maintenant ne prouve pas
que la machine parle** — savoir dire ce que votre code en fait.*

**FOR-6 (bonus) — Tests de topologie**
Implémenter les trois tests de `ForgeTopologyTest` avec **TopologyTestDriver**
(démarche vue en séance 14) : DLQ, rebut non valorisé, machine muette. Ajouter
`kafka-streams-test-utils` en scope test.
*Critères : `mvn test` vert ; le test de FOR-5 fait **avancer le temps sans
envoyer de message** sur la machine surveillée — c'est tout l'intérêt.*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Alimenter les topics d'entrée : le générateur, dans un second terminal,
#    depuis le dossier generateurs-v3/ du dépôt du cours (voir son README)
python generator.py --project forge --create-topics --replication-factor 1

# 3. Lancer en mode dev (live reload)
GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn quarkus:dev
```

```powershell
# Windows PowerShell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn quarkus:dev
```

Vous en avez le droit — l'IA est autorisée dans ce module. Mais sachez ce que vous achetez : ce projet est évalué à
l'oral, code sous les yeux, avec modification en direct et nouvelles exigences métier injectées séance tenante. Un
ticket qui tourne mais que vous ne savez pas expliquer n'est pas crédité.
Demandez-lui d'expliquer chaque choix avant d'écrire une ligne : type de fenêtre, clé d'agrégation, placement de la
jointure, sort des retardataires. C'est mot pour mot ce qu'on vous demandera en soutenance.
Et sachez-le : ce sujet contient des exigences qu'une implémentation produite sans l'avoir lu ne satisfera pas. Elles
sont écrites noir sur blanc, dans le tableau des anomalies et dans les critères de chaque ticket. Le correcteur
automatique les vérifie et les chiffre. Si vous ne les avez pas trouvées, c'est que vous n'avez pas lu.

**Important** : l'extension Quarkus attend que les topics listés dans
`quarkus.kafka-streams.topics` existent avant de démarrer la topologie :
lancez le générateur (étape 2, `--create-topics` la première fois) **avant**
l'application. Il produit en continu le flux du projet, données sales et incidents
compris (`Ctrl+C` pour l'arrêter). Son README détaille les options : débit,
flux propre pour déboguer, mode fichier qui simule 3 h de flux en quelques
secondes.

FOR-5 a besoin d'au moins **40 min d'historique** pour qu'une panne de
passerelle soit tombée. En local, sans attendre, le mode fichier du générateur
simule ces 40 min en quelques secondes :

```bash
python generator.py --project forge --to-dir data/forge --minutes 60
python rejouer.py --dossier data/forge --bootstrap localhost:29092
```

Sur le cluster partagé, il y en a des heures.

## Contraintes

- Java 21, Quarkus + Kafka Streams uniquement (pas de Spark/Flink), pas de
  base externe.
- `GROUPE` obligatoire : topics de sortie et `application.id`
  (`forge-<groupe>`) préfixés — déjà câblé.
- Topics d'entrée partagés en lecture : **ne les modifiez pas**.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                   | Points |
|-----------------------------------------------------------|--------|
| Socle FOR-1 en production 10 min sans crash + DLQ motivée | **8**  |
| FOR-2 (dérive d'outil + seuil de significativité)         | +2     |
| FOR-3 (top par atelier + GlobalKTable)                    | +3     |
| FOR-4 (valeur produite + dédoublonnage)                   | +3     |
| FOR-5 (machine muette)                                    | +2     |
| FOR-6 (tests TopologyTestDriver)                          | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité.** Attendez-vous à
une demande de modification en direct (« Product Owner twist »).

## Conseils

- Le `Log.infof` fourni sert au premier contact : supprimez-le ensuite.
- Poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite le crash,
  la validation métier vous appartient.
- FOR-4 est le cœur métier : réfléchissez à la clé (`machine_id`) et au
  dédoublonnage AVANT l'agrégation (un state store des `unit_id` vus, avec une
  politique d'expiration, est une piste).
- FOR-5 est le morceau, et il ne se traite pas au DSL. Deux questions à vous
  poser dans l'ordre : **qu'est-ce que je retiens** (quel état, quelle clé), et
  **qui me réveille** pour aller le relire. Regardez `process(...)`,
  `context().schedule(...)` et les deux `PunctuationType` — et demandez-vous
  lequel des deux survit à un rejeu de données.
- FOR-5, l'autre moitié : le flux contient des relevés vieux de 30 à 180 min.
  Que fait votre « dernier relevé vu » quand l'un d'eux arrive ?
- FOR-2 : agrégat à deux compteurs (rebuts, total) dans un seul `aggregate`,
  plutôt que deux flux à joindre.
- `split()`/`branch()` — l'ancienne API `branch(Predicate...)` a disparu en
  Kafka 4.0.
