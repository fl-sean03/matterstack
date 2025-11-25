import pytest
from matterstack.ai.surrogate import RandomSurrogate

def test_random_surrogate():
    model = RandomSurrogate(seed=123)
    
    X = [[1.0, 2.0], [3.0, 4.0]]
    y = [10.0, 20.0]
    
    # Must fit before predict
    with pytest.raises(RuntimeError):
        model.predict(X)
        
    model.fit(X, y)
    
    preds = model.predict(X)
    assert len(preds) == 2
    assert isinstance(preds[0], float)
    assert 0.0 <= preds[0] <= 1.0

def test_reproducibility():
    model1 = RandomSurrogate(seed=42)
    model1.fit([], [])
    p1 = model1.predict([[1], [2]])
    
    model2 = RandomSurrogate(seed=42)
    model2.fit([], [])
    p2 = model2.predict([[1], [2]])
    
    assert p1 == p2