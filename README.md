# shap-vs-lime-heart-disease
A visual and code-based comparison of SHAP and LIME explainability techniques on a real-world heart disease classification dataset

# project structure
```
shap-vs-lime-heart-disease/
│
├── data/
│   └── heart.csv                     # Dataset from Kaggle
│
├── notebooks/
│   ├── 1_data_exploration.ipynb      # Initial EDA & preprocessing
│   ├── 2_model_training.ipynb        # Classification model training
│   ├── 3_shap_analysis.ipynb         # SHAP global/local explainability
│   └── 4_lime_analysis.ipynb         # LIME local explainability
│
├── images/
│   └── shap_summary.png              # Saved SHAP summary plot
│   └── lime_explanation.png          # LIME explanation for sample
│   └── comparison_bar.png            # Bar chart comparing SHAP vs LIME fidelity or runtime
│
│
├── shap_vs_lime.py                   # Script version for CLI use 
├── requirements.txt                  # Python dependencies
├── README.md                         # Project overview and visual summary
└── LICENSE
```