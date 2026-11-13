# TP CI/CD – GitHub Actions pour Airflow : réponses

## Partie 1 : mettre le projet sur GitHub

**À quoi sert `git status` avant le commit ?**
Il montre ce qui va partir dans le commit (fichiers suivis, modifiés, non suivis). On vérifie ainsi qu'aucun fichier inutile ou secret n'est ajouté avant de figer le commit.

**Quels fichiers auraient été envoyés sans le `.gitignore` ?**
Le venv (`.venv/`, des milliers de fichiers), les logs d'Airflow (`logs/`), les caches Python (`__pycache__/`, `*.pyc`, `.pytest_cache/`) et surtout `.env`, qui contient des secrets (mots de passe, `AIRFLOW_UID`…).

## Partie 2 : premier workflow « Hello CI »

**Que se passe-t-il si on retire `actions/checkout@v4` ?**
Le runner démarre vide : le code du dépôt n'est pas copié. `ls dags/` échoue (`No such file or directory`) et le job passe au rouge.

**En YAML, que se passe-t-il si l'indentation est fausse ?**
Comme en Python, la structure change ou devient invalide. GitHub affiche « Invalid workflow file » et le workflow ne se lance pas, ou une clé se retrouve rattachée au mauvais parent (une étape hors de `steps`, par exemple).

## Partie 3 : lint avec flake8

**F401** : un module est importé mais jamais utilisé (`'os' imported but unused`). C'est une remarque, pas une erreur bloquante.
**F821** : nom non défini (`undefined name 'X'`). Le code utilise une variable, fonction ou classe qui n'existe pas, souvent à cause d'un import oublié, et plantera à l'exécution. Cette erreur est bloquante (famille F82).

**ruff** : alternative écrite en Rust, 10 à 100 fois plus rapide. Elle reprend les règles flake8 et ajoute des règles `AIR` propres à Airflow (par exemple les imports dépréciés en Airflow 3).

## Partie 4 : tester l'import des DAGs avec pytest

Version d'Airflow du projet : **3.1.0** (`image: apache/airflow:3.1.0` dans `docker-compose.yaml`).

Assert complété dans `tests/test_dags.py` :
```python
assert taches == attendues
```

**À quoi sert `needs: lint` ?**
Il crée une dépendance : `test-dags` attend la fin de `lint` et ne démarre que s'il est vert. Si le lint échoue, `test-dags` est marqué *skipped* (non exécuté). On évite ainsi 2 à 3 minutes d'installation d'Airflow pour un code dont on sait déjà qu'il est cassé.

**Pourquoi installer Airflow avec un fichier de constraints ?**
Airflow a des centaines de dépendances. Le fichier de constraints fixe pour chacune la version exacte testée par l'équipe Airflow, pour une version d'Airflow et de Python données. Sans lui, pip prendrait les dernières versions disponibles : on risquerait des incompatibilités (une lib qui casse Airflow), une CI qui passe un jour et échoue le lendemain sans changement de code, ou un environnement différent de celui de l'image Docker.

**Pourquoi installer `apache-airflow-providers-postgres` dans la CI alors qu'il est dans l'image Docker ?**
La CI n'utilise pas l'image Docker : elle installe Airflow avec pip sur un runner Ubuntu vierge. `pip install apache-airflow` n'installe que le cœur, sans ce provider. Sans lui, `from airflow.providers.postgres...` lève `ModuleNotFoundError` et le DagBag signale une erreur d'import.

## Partie 5 : casser un DAG, lire l'échec, corriger

| Cas | Modification | Job qui échoue | Message à retrouver |
|---|---|---|---|
| A | Supprimer l'import de `PostgresHook` | `lint` (étape « Erreurs bloquantes ») ; `test-dags` est *skipped* | `F821 undefined name 'PostgresHook'` |
| B | Supprimer l'appel final `user_processing()` | `test-dags` (le lint est vert) | `test_user_processing_existe` : `AssertionError: assert 'user_processing' in {}` ; `test_user_processing_taches` : `KeyError: 'user_processing'` → `2 failed, 1 passed` |
| C | Renommer `store_user` en `save_user` | `test-dags` (le lint est vert) | `test_user_processing_taches` : `AssertionError: Tâches manquantes : {'store_user'} ; tâches en trop : {'save_user'}` → `1 failed, 2 passed` |

Le cas A a été vérifié en local avec flake8 (`F821 undefined name 'PostgresHook'`, code retour 1). Le cas B aussi : flake8 ne renvoie rien.

**Cas B : pourquoi flake8 ne voit-il rien ?**
Le fichier reste du Python parfaitement valide : la fonction décorée `@dag` est définie, mais jamais appelée. flake8 analyse le code sans l'exécuter, et il n'y a ni nom inconnu ni erreur de syntaxe. Seule l'exécution (le DagBag) révèle qu'aucun objet DAG n'est créé.

**Cas C : le DAG fonctionnerait, pourquoi est-ce utile que le test échoue ?**
Le `task_id` fait partie du contrat : renommer une tâche fait perdre son historique dans Airflow (anciens runs, logs) et casse ce qui en dépend : XCom lus par `task_id`, capteurs externes, alertes et tableaux de bord. Le test rend le changement visible et intentionnel : s'il est voulu, on met à jour le test ; sinon, on a évité une régression silencieuse.

**Combien de temps pour trouver l'erreur du cas B dans l'interface Airflow ?**
Longtemps : il n'y a **aucune erreur affichée**. Le DAG disparaît simplement de la liste, sans rien dans « DAG Import Errors ». Il faut attendre le rescan du DAG processor (jusqu'à plusieurs minutes), constater l'absence, puis relire le fichier pour chercher pourquoi. Il faut compter de plusieurs minutes à beaucoup plus, contre environ 3 minutes avec un message explicite dans la CI.

## Pour aller plus loin : pull request

**Le binôme pourrait-il valider la PR à votre place ?**
Il peut et doit relire puis approuver la PR, mais l'auteur ne peut pas s'approuver lui-même : GitHub l'interdit quand la branche exige une revue. C'est justement l'intérêt d'un regard extérieur.

**Intérêt d'une relecture humaine en plus de la CI ?**
La CI ne vérifie que ce qu'on lui a demandé (syntaxe, import, liste des tâches). L'humain juge la logique métier (bonne requête SQL ? bonne planification ?), la lisibilité, la sécurité (secret en dur, connexion de prod) et la pertinence du changement. Il partage aussi la connaissance du code dans l'équipe.

## Bilan

1. **Deux erreurs que cette CI aurait détectées** : un import oublié ou mal orthographié (par exemple `PostgresHook` non importé, ou un mauvais chemin `airflow.providers...`), et un DAG qui n'apparaît pas dans l'interface parce que la fonction `@dag` n'était pas appelée. On peut aussi citer un provider manquant (`ModuleNotFoundError`) ou une faute de frappe dans un `task_id`.
2. **Pourquoi le lint avant les tests ?** Le lint est quasi instantané (quelques secondes) et attrape les erreurs grossières. Les tests demandent d'installer Airflow (2 à 3 minutes). En mettant le lint en premier avec `needs: lint`, on échoue vite et on ne gaspille pas de minutes de runner sur du code déjà cassé.
3. **En MLOps, la CI pourrait vérifier en plus du code** : la qualité et le schéma des données (colonnes, types, valeurs manquantes, dérive) ; le modèle lui-même : métriques minimales sur un jeu de test (accuracy, F1, RMSE ≥ seuil), absence de régression par rapport au modèle en production, temps d'inférence et taille, chargement correct de l'artefact sérialisé, reproductibilité de l'entraînement et équité ou biais sur des sous-groupes.
