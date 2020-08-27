#!/usr/local/bin/python3
# encoding=utf-8

import json

import synonyms
from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/synonyms", methods=["GET", "POST"])
def get_synonyms():
    if request.method == "GET":
        word = request.args.get("word", "")
        if word == "":
            return jsonify({"data": "请输入要查询的词"})
        words, scores = synonyms.nearby(word)
        size = len(words)
        lists = []
        for i in range(size):
            dicts = {"word": words[i], "score": str(scores[i])}
            lists.append(dicts)

        return json.dumps(lists)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)
