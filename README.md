# Tyrimas – Funkcinė Gaia XP spektrų analizė

Šiame repozitorijoje pateikiami notebook’ai ir įrankiai, skirti Gaia DR3 XP spektrinių duomenų analizei, orientuojantis į funkcinių duomenų analizę (FDA) ir interpretuojamus klasifikavimo metodus.

## Struktūra

- **01_sampled_fpca_distance_models.ipynb**  
  Skirtas pradinei spektrinių duomenų analizei, kurioje taikomi atstumu paremti klasifikatoriai bei FPCA pagrindu suformuotos reprezentacijos. Šiame etape įvertinamas paprastesnių metodų veikimas.

- **02_fpca_vs_fpls.ipynb** *(priedas 4)*  
  Naudojamas funkcinių dimensijos mažinimo metodų palyginimui. Jame analizuojami FPCA ir FPLS metodai bei vertinama jų įtaka klasifikavimo rezultatams.

- **03_rkhs_robust.ipynb** *(priedas 5)*  
  Skirtas papildomų, sudėtingesnių metodų (kernelinių modelių) testavimui bei jų rezultatų palyginimui su interpretuojamais modeliais.

- **04_final_functional.ipynb** *(priedas 6)*  
  Pagrindinis analizės failas, kuriame atliekamas galutinis skirtingų modelių vertinimas ir palyginimas. Šiame faile sujungiami visų metodų rezultatai ir sudaromos galutinės palyginimo lentelės.

- **05_functional_interpretability.ipynb** *(priedas 7)*  
   Skirtas modelių interpretuojamumo analizei. 

# astroflow_project – Praktikos metu sukurtas paketas *(priedas 8)*  
  Praktikos metu sukurtas Python paketas, skirtas:
  - Gaia duomenų atsisiuntimui (TAP)
  - koordinačių sulyginimui (cross-match)
  - XP spektrų gavimui
  - duomenų paruošimui analizei
