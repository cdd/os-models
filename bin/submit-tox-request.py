import requests
import json

if __name__ == '__main__':
    test_request = {'model': 'Consensus', 'smiles': ['c1ccccc1O',
                                                     'CC(=O)O[C@H]1C[C@H](O[C@H]2[C@@H](O)C[C@H](O[C@H]3[C@@H](O)C[C@H](O[C@H]4CC[C@]5(C)[C@H]6CC[C@]7(C)[C@@H](C8=CC(=O)OC8)CC[C@]7(O)[C@@H]6CC[C@@H]5C4)O[C@@H]3C)O[C@@H]2C)O[C@H](C)[C@H]1O'
               ]}
    json_request = json.dumps(test_request)
    info = requests.post('http://localhost:8200/tox21Activity', json=test_request)
    response = info.json()

    print(response)
