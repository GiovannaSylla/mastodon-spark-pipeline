# viz_part5.py
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

DATA = Path("data")        # volume host monté dans les conteneurs
OUT  = Path("reports")     # où on écrit les images
OUT.mkdir(exist_ok=True)

# --- 1) Distribution globale (bar chart) ---
dist = pd.read_csv(DATA / "viz_sentiment_dist.csv")
plt.figure()
plt.bar(dist["label_str"], dist["n"])
plt.title("Répartition des sentiments")
plt.xlabel("Sentiment")
plt.ylabel("Nombre de toots")
plt.tight_layout()
plt.savefig(OUT / "part5_sentiment_distribution.png")
plt.close()

# --- 2) Toots par jour + % positif/négatif/neutre (2 graphes simples) ---
daily = pd.read_csv(DATA / "viz_sentiment_daily.csv", parse_dates=["day"])
daily = daily.sort_values("day")

# toots par jour (line)
plt.figure()
plt.plot(daily["day"], daily["n_toosts"])
plt.title("Nombre de toots par jour")
plt.xlabel("Jour")
plt.ylabel("Toots")
plt.tight_layout()
plt.savefig(OUT / "part5_toots_per_day.png")
plt.close()

# Stacked bars sentiments par jour 
plt.figure()
width = 0.8
plt.bar(daily["day"], daily["n_pos"], width, label="Positif")
plt.bar(daily["day"], daily["n_neu"], width, bottom=daily["n_pos"], label="Neutre")
bottom = daily["n_pos"] + daily["n_neu"]
plt.bar(daily["day"], daily["n_neg"], width, bottom=bottom, label="Négatif")
plt.title("Répartition des sentiments par jour")
plt.xlabel("Jour")
plt.ylabel("Toots")
plt.legend()
plt.tight_layout()
plt.savefig(OUT / "part5_sentiment_by_day_stacked.png")
plt.close()

# --- 3) Top hashtags (horizontal bar chart) ---
tags = pd.read_csv(DATA / "viz_top_hashtags.csv")
tags = tags.sort_values("n", ascending=True)  
plt.figure(figsize=(8, max(4, len(tags)*0.25)))
plt.barh(tags["hashtag"], tags["n"])
plt.title("Top hashtags")
plt.xlabel("Nombre de toots")
plt.ylabel("Hashtag")
plt.tight_layout()
plt.savefig(OUT / "part5_top_hashtags.png")
plt.close()

print("Graphes écrits dans ./reports :")
for p in sorted(OUT.glob("part5_*.png")):
    print(" -", p)
