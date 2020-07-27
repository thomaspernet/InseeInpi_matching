---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Test Word2Vec

```python
import pandas as pd
import re
#from nltk.corpus import stopwords
import matplotlib.pyplot as plt
#from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from gensim.models import Word2Vec
```

```python
df = pd.read_csv('test_ngrams.csv').sort_values('CNT', ascending = False)
```

```python
df.head()
```

```python
def basic_clean(text):
    return re.sub(r'[^\w\s]|[|]', '', text).split()
df_text = df['unique_combinaition'].apply(lambda x: basic_clean(x))
```

```python
model = Word2Vec(df_text.tolist(), min_count=1)
```

```python
model.wv.similarity('BOULEVARD', 'BD')
```

```python
list(
    map(
        lambda x:
        model.wv.similarity('APPART', x),
        ['HLM', 'PRE', 'ROND', 'CHEZ', 'MME', 'MARTHE', 'AUZANCE']
    )
)
```

```python
list(
    map(
        lambda x:
        model.wv.similarity('CDT', x),
        ['BIS', 'RUE', 'COMMANDANT', 'LEVRARD']
    )
)
```

```python
list(
    map(
        lambda x:
        model.wv.similarity('MATIISE', x),
        ['ANGLE', 'RUE', 'HENRI', 'MATISSE', 'PAUL', 'CEZANNE', 'AULNAY', 'SOUS', 'BOIS']
    )
)
```

```python
import nltk
```

```python
list(
    map(
        lambda x:
        nltk.edit_distance('MATIISE', x),
        ['ANGLE', 'RUE', 'HENRI', 'MATISSE', 'PAUL', 'CEZANNE', 'AULNAY', 'SOUS', 'BOIS']
    )
)
```
