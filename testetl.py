from failuremail import get_data

def test():
    result=get_data()
    assert isinstance(result,dict)
    assert result["age"]==21
    assert result["name"]=="shruti"