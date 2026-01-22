## PROJECT OJECTIVE

1. Raccolta dei dati a partire da richieste in linguaggio naturale
Questa fase si occupa di interpretare le richieste dell’utente, espresse in linguaggio naturale, e di tradurle in specifiche tecniche per l’estrazione dei dati. L’obiettivo è identificare quali dati (ad esempio, quali tabelle o elementi della campagna) sono necessari per soddisfare la richiesta, rispettando sempre le regole di allowlist. In un’architettura agentica, questa logica può essere orchestrata tramite agenti LangChain/LangGraph che, sfruttando modelli LLM come Gemini, mappano l’intento dell’utente su azioni concrete e deterministiche, senza mai eseguire calcoli numerici autonomi.

2. Processing dei dati tramite funzioni deterministiche (tools/MCP)
Una volta identificati i dati richiesti, la seconda macro-area si occupa dell’elaborazione vera e propria, utilizzando funzioni deterministiche già presenti nel sistema (ad esempio, il modulo di post-processing MCP). Questa fase prevede la selezione e il filtraggio delle configurazioni (element_config, label_attribute, label_objects), l’esecuzione delle pipeline di post-processing sulle fixture locali e la generazione degli output in formato JSON. È fondamentale che questa fase sia completamente tracciabile (ad esempio tramite LangSmith) e che i risultati siano sempre confrontabili con i dati di riferimento, per garantire la correttezza e la riproducibilità.

3. Elaborazione dei risultati e costruzione del report tecnico
L’ultima macro-area si concentra sulla sintesi e presentazione dei risultati cioè, quindi, generazione di report.
Preferibile modelli LLM con free tier, gemini / groq