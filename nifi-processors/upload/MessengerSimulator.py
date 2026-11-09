from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import json
import random
import string
import datetime
from datetime import timezone

# Static word pools (no Faker) to keep NiFi processor venv small on limited disk.
_TWEET_WORDS = (
    "vessel", "convoy", "Baltic", "patrol", "AIS", "harbour", "sanctioned",
    "cargo", "frigate", "submarine", "NATO", "shipping", "lane", "radar",
    "exercise", "deployment", "maritime", "surveillance", "port", "fleet",
)
_FIRST_NAMES = ("Alex", "Jordan", "Morgan", "Sam", "Chris", "Robin", "Taylor")
_LAST_NAMES = ("Nordsee", "BalticWatch", "SeaGuard", "KielObserver", "MarineEye")

class TwitterMessageGenerator(FlowFileTransform):
    """
    Ein NiFi-Python-Prozessor, der eine konfigurierbare Anzahl von Twitter-ähnlichen
    Nachrichten mit zufälligem Text, Benutzernamen und Metriken generiert.
    
    Generiert zufällige JSON-Daten ohne externe Pip-Abhängigkeiten (stdlib only).
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.6.0'
        description = 'Generiert eine konfigurierbare Anzahl von Twitter-ähnlichen JSON-Nachrichten mit zufälligem Text, Benutzernamen und Metriken. Er ignoriert den eingehenden FlowFile-Inhalt.'
        dependencies = []

    # Definiert eine Eigenschaft, um die maximale Anzahl der zu generierenden Nachrichten festzulegen.
    MAX_OUTPUT_MESSAGES = PropertyDescriptor(
        name="Max Output Messages",
        description="Die maximale Anzahl von Twitter-Nachrichten, die pro eingehendem FlowFile generiert werden sollen. Ein Wert von 0 oder eine leere Angabe bedeutet keine Begrenzung.",
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        default_value="10", # Standardwert für die Anzahl der Nachrichten
        required=False
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.MAX_OUTPUT_MESSAGES]

    def getPropertyDescriptors(self):
        return self.descriptors

    def _generate_priority(self):
        """
        Generiert eine Priorität basierend auf der angegebenen Verteilung.
        98 % niedrig, 1 % mittel, 1 % hoch.
        """
        roll = random.randint(1, 100)
        if roll <= 1:
            return "hoch"
        elif roll <= 2:
            return "mittel"
        else:
            return "niedrig"
            
    def _generate_baltic_coordinates(self):
        """
        Generiert zufällige Breitengrad- und Längengrad-Koordinaten in der Ostseeregion.
        Begrenzungsrahmen für die Ostsee:
        Breitengrad: 53.5° N bis 65.8° N
        Längengrad: 9.5° E bis 30.2° E
        """
        min_lat, max_lat = 53.5, 65.8
        min_lon, max_lon = 9.5, 30.2
        
        latitude = round(random.uniform(min_lat, max_lat), 4)
        longitude = round(random.uniform(min_lon, max_lon), 4)
        
        return {"latitude": latitude, "longitude": longitude}

    def _generate_random_tweet_text(self):
        """
        Generiert einen zufälligen, Twitter-ähnlichen Text mit Hashtags und Emojis.
        """
        n_words = random.randint(5, 20)
        text = " ".join(random.choice(_TWEET_WORDS) for _ in range(n_words)).capitalize() + "."

        # Listen mit möglichen Hashtags und Emojis, aktualisiert für militärische/marinebezogene Themen
        hashtags = ["#suspect", "#vessel", "#container", "#uboot", "#sanctioned", "#Marine", "#NATO", "#Militär", "#Seelogistik"]
        emojis = ["⚓", "🪖", "🎖️", "🚤", "🛰️"] # Aktualisierte Emojis
        
        # Wählt eine zufällige Anzahl eindeutiger Hashtags und Emojis aus
        num_hashtags = random.randint(1, 3)
        num_emojis = random.randint(0, 2)
        selected_hashtags = random.sample(hashtags, k=min(num_hashtags, len(hashtags)))
        selected_emojis = random.sample(emojis, k=min(num_emojis, len(emojis)))
        
        # Fügt die zufällig ausgewählten Elemente zum Text hinzu
        full_text = text.strip() + " " + " ".join(selected_hashtags) + " " + " ".join(selected_emojis)
        
        # Schneidet den Text auf die maximale Tweet-Länge ab
        if len(full_text) > 280:
            full_text = full_text[:277].strip() + "..."

        return full_text

    def transform(self, context, flowFile):
        """
        Die Hauptmethode des Prozessors. Sie ignoriert das eingehende FlowFile und
        generiert eine konfigurierbare Anzahl von neuen Tweets.
        """
        try:
            output_records = []
            
            # Liest die konfigurierte Eigenschaft aus oder verwendet den Standardwert
            max_messages_str = context.getProperty(self.MAX_OUTPUT_MESSAGES.name).getValue()
            max_messages = int(max_messages_str) if max_messages_str and max_messages_str.isdigit() else 10

            # Generiert die angeforderte Anzahl von Nachrichten
            for _ in range(max_messages):
                tweet_text = self._generate_random_tweet_text()
                
                base_user = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
                tweet_record = {
                    "user_name": f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}",
                    "user_username": f"user_{base_user}",
                    "tweet": tweet_text,
                    "ts": datetime.datetime.now(timezone.utc).isoformat(), 
                    "priority": self._generate_priority(),
                    "latitude": self._generate_baltic_coordinates()['latitude'],
                    "longitude": self._generate_baltic_coordinates()['longitude'],
                    "metrics_retweets": random.randint(0, 500),
                    "metrics_likes": random.randint(10, 2000),
                    "metrics_replies": random.randint(0, 50)
                }
                output_records.append(tweet_record)

            # Vereinigt alle generierten JSON-Datensätze im NDJSON-Format.
            output_content_string = "\n".join([json.dumps(record) for record in output_records])

            # Gibt das Ergebnis als neues FlowFile zurück
            return FlowFileTransformResult(
                contents=output_content_string.encode("utf-8"),
                attributes={"format": "json", "tweet.generated": "true", "simulated.twitter.fields": "true"},
                relationship="success"
            )

        except Exception as e:
            self.logger.error(f"Fehler beim Generieren der Twitter-Nachrichten: {e}")
            return FlowFileTransformResult(
                contents=f"Fehler: {e}".encode("utf-8"),
                attributes={"error": str(e), "tweet.generation.error": "true"},
                relationship="failure"
            )

