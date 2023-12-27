## Introduction

Cet outil est conçu pour synchroniser les données entre une base de données Postgresql et Elasticsearch. Il utilise un
système de trigger avec un mécanisme de pub/sub pour surveiller les changements en temps réel dans la base de données et
les indexer dans Elasticsearch.

## Prérequis

- Docker
- Docker Compose

## Installation et configuration

1. Cloner ce repository: `git clone https://github.com/alancolant/pg_el_sync pgsync`
2. Aller dans le répertoire `pgsync`: `cd pgsync`
3. Copier le fichier `config.example.yaml` vers `config.yaml`: `cp config.example.yaml config.yaml`
3. Modifier le fichier de configuration `config.yml` pour spécifier les détails de connexion à Postgresql et
   Elasticsearch, ainsi que les tables à synchroniser et leurs relations.
4. Lancer le service Docker-Compose: `docker compose up -d`

## Utilisation

Une fois que le service Docker Compose est lancé, vous pouvez utiliser les commandes suivantes pour synchroniser les
données:

- `docker compose up -d prod`: Cette commande surveille la base de données en temps réel et synchronise les données dès
  qu'il y a des changements.
- `docker compose exec prod pgsync index`: Cette commande indexe toutes les tables spécifiées dans la configuration.
