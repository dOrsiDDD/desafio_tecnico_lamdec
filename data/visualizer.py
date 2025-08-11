import pandas as pd

df = pd.read_csv("data/001.csv")
#print(df['numCDA'].duplicated().sum())
df4 = pd.read_csv("data/004.csv")
df5 = pd.read_csv("data/005.csv")
#df_duplicado = (df[df['numCDA'].duplicated(keep=False)])
#df4_duplicado = (df4[df4['numCDA'].duplicated(keep=False)])
#df5_duplicado = (df5[df5['numCDA'].duplicated(keep=False)])

df_pf = pd.read_csv("data/006.csv")
df_pj = pd.read_csv("data/007.csv")

df_pessoas = pd.concat([df_pf, df_pj], ignore_index=True)
df_pessoas_unicas = df_pessoas.drop_duplicates(subset=["idpessoa"], keep="first")
df_pessoas_unicas.to_csv("data/pessoas_unicas.csv", index=False, encoding='utf-8-sig')



#duplicados_pessoas = df_pessoas[df_pessoas.duplicated(subset=["idpessoa"], keep=False)]
#duplicados_pessoas.to_csv("data/idpessoa_duplicados.csv", index=False, encoding='utf-8-sig')


#df_duplicado.to_csv("data/duplicados.csv", index=False)
#df4_duplicado.to_csv("data/duplicados4.csv", index=False, encoding='utf-8-sig')
#df5_duplicado.to_csv("data/duplicados5.csv", index=False, encoding='utf-8-sig')

