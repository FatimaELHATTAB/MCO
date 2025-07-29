 Arbitrage sur le point d’exposition de l’API TVA intra-communautaire

Bonjour 

Avec Manel, nous avions prévu d’échanger avec toi avant son départ sur l’exposition des données de TVA intra-communautaire, que nous devons rendre accessibles à d’autres entités du groupe BNP. Je te partage donc les éléments pour arbitrage.

Les données sont traitées par notre équipe UMD.
Nous souhaitons exposer une API pour permettre leur consommation, mais un arbitrage est nécessaire quant au point d’exposition : faut-il le faire directement depuis UMD, via APIGEE, ou attendre une exposition centralisée par TPN ?

À date, TPN a indiqué qu’ils resteront sur un mode tactique jusqu’en 2026, sans visibilité claire sur le démarrage de leur roadmap sur ce sujet. Cela nous pousse à envisager des alternatives pour ne pas bloquer la mise à disposition.

Voici les trois scénarios envisagés :

1. Exposition via UMD + APIGEE

Avantages : permet d’exposer rapidement l’API sans attendre TPN ; pas de déportation à prévoir pour les consommateurs lors de la migration vers TPN

Inconvénients : nécessite un passage par APIGEE (développement supplémentaire), donc mise en œuvre un peu plus longue que la cible UMD directe

2. Exposition via TPN (solution cible)

Avantages : conforme à l’architecture cible 4ALL

Inconvénients : les développements n’ont pas commencé, la charge est importante, et nous n’avons aucune visibilité sur un atterrissage court terme

3. Exposition directe via UMD (sans APIGEE)

Avantages : très rapide à mettre en place

Inconvénients : les clients devront se reconnecter une fois la migration vers TPN effectuée

Nous avons besoin de ton arbitrage pour pouvoir avancer sereinement sur ce sujet.

Bonne journée,
Fatima
