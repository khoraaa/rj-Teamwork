"""通用符号切分器的回归测试（纯标准库，不启动服务）。

跑法：python tools\\split_symbols_test.py
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from symbols import split_symbols  # noqa: E402


CASES = []


def case(name, filename, text, expect_names=(), expect_absent=(), strategy=None):
    CASES.append((name, filename, text, expect_names, expect_absent, strategy))


PY = '''"""模块说明：这里有 {} 和 /* */ 之类的假括号。"""

CONFIG = {"a": 1}

class UserService:
    """用户服务 {不是代码}。"""

    def __init__(self, db):
        self.db = db

    def create(self, name):
        return self.db.insert(name)


def bootstrap():
    return UserService(None)
'''
case("python 类与方法", "svc.py", PY,
     expect_names=("UserService", "create", "bootstrap"), strategy="indent")

JS = '''const s = "class Fake { not real }";
// function nope() { }
export class Cart {
  constructor() { this.items = []; }
  add(item) {
    const label = `item ${item.id} {`;
    return this.items.push(label);
  }
}

function total(cart) {
  return cart.items.length;
}
'''
case("js 类/方法/模板字符串", "cart.js", JS,
     expect_names=("Cart", "add", "total"), expect_absent=("Fake", "nope"))

JAVA = '''package demo;

public class OrderService {
    private final Repo repo;

    public OrderService(Repo repo) {
        this.repo = repo;
    }

    public Order create(String user) {
        if (user == null) { throw new IllegalArgumentException("user"); }
        return repo.save(new Order(user));
    }
}
'''
case("java 类与方法", "OrderService.java", JAVA,
     expect_names=("OrderService", "create"))

GO = '''package main

type Server struct {
    Port int
}

func (s *Server) Start() error {
    return nil
}

func NewServer(port int) *Server {
    return &Server{Port: port}
}
'''
case("go 结构体与函数", "server.go", GO,
     expect_names=("Server", "Start", "NewServer"))

SQL = '''CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE VIEW active_users AS
SELECT * FROM users WHERE deleted_at IS NULL;
'''
case("sql 表与视图", "schema.sql", SQL,
     expect_names=("users", "active_users"), strategy="sql")

RUBY = '''class Greeter
  def hello(name)
    "hi #{name}"
  end

  def bye
    "bye"
  end
end
'''
case("ruby 类与方法", "greeter.rb", RUBY,
     expect_names=("Greeter", "hello", "bye"), strategy="endkw")

BROKEN = '''public class Broken {
    void go() {
        int x = 1;
'''
case("括号不闭合要降级", "Broken.java", BROKEN, expect_names=("Broken",))

MINIFIED = ("var a=function(){return 1};" * 400)
case("单行压缩文件不炸", "bundle.min.js", MINIFIED)

EMPTY = ""
case("空文件", "empty.py", EMPTY)

STRINGS = '''def f():
    s = ")))"
    t = '{'
    return s + t
'''
case("字符串里的括号不干扰", "brackets.py", STRINGS, expect_names=("f",))


def check(entry):
    name, filename, text, expect, absent, strategy = entry
    info = split_symbols(text, filename)
    symbols = info["symbols"]
    names = [s["name"] for s in symbols]
    lines = text.count("\n") + 1 if text else 0
    for sym in symbols:
        assert 1 <= sym["start"] <= sym["end"], (name, sym)
        assert sym["end"] <= max(lines, 1), (name, sym, lines)
        assert sym["name"], (name, sym)
        assert sym["kind"], (name, sym)
    for want in expect:
        assert want in names, "%s：缺少符号 %s（实际 %s）" % (name, want, names)
    for bad in absent:
        assert bad not in names, "%s：误切出 %s" % (name, bad)
    if strategy:
        assert info["strategy"] == strategy, "%s：策略 %s != %s" % (
            name, info["strategy"], strategy)
    return info


def main():
    failures = []
    for entry in CASES:
        try:
            info = check(entry)
            print("ok   %-22s strategy=%-7s conf=%-6s symbols=%s" % (
                entry[0], info["strategy"], info["confidence"],
                len(info["symbols"])))
        except AssertionError as exc:
            failures.append(str(exc))
            print("FAIL %-22s %s" % (entry[0], exc))

    # 准确性：每个符号的范围要覆盖它自己的名字
    info = split_symbols(PY, "svc.py")
    create = next(s for s in info["symbols"] if s["name"] == "create")
    assert create["start"] < create["end"], "方法范围应跨行"
    assert create["parent"] == "UserService", "方法应挂在类下面：%s" % create["parent"]

    info = split_symbols(JS, "cart.js")
    add = next(s for s in info["symbols"] if s["name"] == "add")
    body = "\n".join(JS.split("\n")[add["start"] - 1:add["end"]])
    assert "this.items.push" in body, "add 的代码块不完整：%r" % body

    if failures:
        print("\n%d 条失败" % len(failures))
        for item in failures:
            print(" -", item)
        return 1
    print("\n全部通过（%d 组）" % len(CASES))
    return 0


if __name__ == "__main__":
    sys.exit(main())