import json
from typing import Dict, List

class SmartActivityScorer:
    """
    Système de scoring INTELLIGENT avec scores corrélés
    Critères : nature, gastronomie, culture, detente, sport
    """

    def __init__(self):
        self.activity_profiles = {

            'hebergement': {
                'primary_keywords': {
                    'schema:accommodation': 10,
                    'schema:lodgingbusiness': 10,
                    'schema:hotel': 10,
                    'hotel': 10,
                    'camping': 9,
                    'gite': 9,
                },
                'correlated_scores': {
                    'nature': 3,
                    'gastronomie': 3,
                    'culture': 0,
                    'detente': 8,
                    'sport': 0,
                }
            },

            'restaurant': {
                'primary_keywords': {
                    'schema:foodestablishment': 10,
                    'schema:restaurant': 10,
                    'restaurant': 10,
                    'auberge': 9,
                    'brasserie': 8,
                },
                'correlated_scores': {
                    'nature': 2,
                    'gastronomie': 10,
                    'culture': 2,
                    'detente': 6,
                    'sport': 0,
                }
            },

            'randonnee': {
                'primary_keywords': {
                    'walkingtour': 10,
                    'sentier': 10,
                    'parcours': 9,
                    'circuit': 9,
                },
                'correlated_scores': {
                    'nature': 10,
                    'gastronomie': 0,
                    'culture': 1,
                    'detente': 7,
                    'sport': 8,
                }
            },

            'culture': {
                'primary_keywords': {
                    'culturalsite': 10,
                    'museum': 10,
                    'musee': 10,
                    'chateau': 10,
                    'eglise': 9,
                },
                'correlated_scores': {
                    'nature': 2,
                    'gastronomie': 1,
                    'culture': 10,
                    'detente': 6,
                    'sport': 0,
                }
            },

            'sport': {
                'primary_keywords': {
                    'sportsandleisureplace': 10,
                    'vtt': 10,
                    'ski': 10,
                    'escalade': 10,
                    'tennis': 9,
                },
                'correlated_scores': {
                    'nature': 6,
                    'gastronomie': 0,
                    'culture': 0,
                    'detente': 2,
                    'sport': 10,
                }
            },

            'nature': {
                'primary_keywords': {
                    'lac': 10,
                    'montagne': 10,
                    'parc': 9,
                    'foret': 10,
                },
                'correlated_scores': {
                    'nature': 10,
                    'gastronomie': 0,
                    'culture': 1,
                    'detente': 8,
                    'sport': 3,
                }
            },

            'spa': {
                'primary_keywords': {
                    'spa': 10,
                    'balneo': 10,
                    'balnéo': 10,
                },
                'correlated_scores': {
                    'nature': 0,
                    'gastronomie': 0,
                    'culture': 0,
                    'detente': 10,
                    'sport': 1,
                }
            },

            'marche': {
                'primary_keywords': {
                    'market': 10,
                    'marche': 10,
                },
                'correlated_scores': {
                    'nature': 5,
                    'gastronomie': 7,
                    'culture': 5,
                    'detente': 4,
                    'sport': 0,
                }
            }
        }

        self.scoring_criteria = [
            'nature',
            'gastronomie',
            'culture',
            'detente',
            'sport'
        ]

    def normalize_text(self, text: str) -> str:
        if not text:
            return ""
        text = text.lower()
        accents = {
            'à': 'a', 'â': 'a', 'ä': 'a',
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'î': 'i', 'ï': 'i',
            'ô': 'o', 'ö': 'o',
            'ù': 'u', 'û': 'u', 'ü': 'u',
            'ç': 'c', 'œ': 'oe',
        }
        for a, r in accents.items():
            text = text.replace(a, r)
        return text

    def detect_activity_type(self, activity: Dict) -> List[str]:
        detected_types = []

        label = self.normalize_text(activity.get('label', ''))
        types = [self.normalize_text(t) for t in activity.get('types', [])]

        for profile_name, profile_data in self.activity_profiles.items():
            keywords = profile_data['primary_keywords']

            # Le profil NATURE ne doit PAS être détecté via les types génériques
            if profile_name != 'nature':
                for activity_type in types:
                    for keyword in keywords:
                        if self.normalize_text(keyword) in activity_type:
                            detected_types.append(profile_name)
                            break

            # Tous les profils peuvent être détectés via le label
            for keyword in keywords:
                if self.normalize_text(keyword) in label:
                    if profile_name not in detected_types:
                        detected_types.append(profile_name)
                    break

        return detected_types

    def score_activity(self, activity: Dict) -> Dict:
        activity_types = self.detect_activity_type(activity)
        scores = {c: 0 for c in self.scoring_criteria}

        if not activity_types:
            scores['detente'] = 3
        else:
            for activity_type in activity_types:
                correlated = self.activity_profiles[activity_type]['correlated_scores']
                for criterion, score in correlated.items():
                    scores[criterion] = max(scores[criterion], score)

        # Les lignes de calcul du score 'global' ont été supprimées ici

        activity = activity.copy()
        activity['scores'] = scores
        activity['detected_types'] = activity_types
        return activity

    def score_activities_file(self, input_file: str, output_file: str):
        print(f" Lecture du fichier {input_file}...")

        with open(input_file, 'r', encoding='utf-8') as f:
            activities = json.load(f)

        print(f"✓ {len(activities)} activités chargées")
        print(" Scoring intelligent en cours...\n")

        scored = []
        for i, activity in enumerate(activities):
            scored.append(self.score_activity(activity))
            if (i + 1) % 500 == 0:
                print(f"  ✓ {i + 1}/{len(activities)} scorées")

        print(f"\n Écriture du fichier {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(scored, f, ensure_ascii=False, indent=2)

        print("Terminé!")

def main():
    import sys
    scorer = SmartActivityScorer()

    if len(sys.argv) >= 3:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
    else:
        input_file = 'Auvergne.json'
        output_file = 'Auvergne_scored.json'
        print("Usage: python smart_scorer.py <input_file> <output_file>\n")

    scorer.score_activities_file(input_file, output_file)

if __name__ == "__main__":
    main()