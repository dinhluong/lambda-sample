import json

def getCredentialJson():
    """
    description:
        認証用ファイル(json)の読み込み
    args:
        in_json : 認証用のjsonファイル名
    returns:
        credential : 読み込んだ認証情報
    """

    with open("./authentication.json", "r") as f:
        credential = json.load(f)
    return credential
