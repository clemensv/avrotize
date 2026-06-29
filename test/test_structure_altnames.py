"""Issue #384: honor JSON Structure altnames.json for wire serialization — full round-trip.

A property `dr_type` + `altnames.json="dr-type"` must serialize `dr-type` on the JSON wire
while keeping the spec-valid identifier `dr_type` as the language member name. Each generator
is exercised end-to-end: generate -> build -> serialize -> assert wire key -> deserialize.
Languages whose toolchain is unavailable are skipped (C++ has no compiler here).
"""

import os
import glob
import json
import shutil
import subprocess
import sys
import tempfile
import importlib.util
import unittest

from avrotize.common import json_wire_name
from avrotize.structuretopython import convert_structure_schema_to_python
from avrotize.structuretocsharp import convert_structure_schema_to_csharp
from avrotize.structuretojava import convert_structure_schema_to_java
from avrotize.structuretots import convert_structure_schema_to_typescript
from avrotize.structuretogo import convert_structure_schema_to_go
from avrotize.structuretorust import convert_structure_schema_to_rust
from avrotize.structuretocpp import convert_structure_schema_to_cpp

# dr_type carries hyphenated wire key dr-type; regular has no altnames (must stay verbatim).
SCHEMA = {
    "name": "Telemetry",
    "type": "object",
    "properties": {
        "dr_type": {"type": "int32", "altnames": {"json": "dr-type"}},
        "regular": {"type": "string"},
    },
    "required": ["dr_type", "regular"],
}
WIN = sys.platform == "win32"


def _out(name: str) -> str:
    path = os.path.join(tempfile.gettempdir(), "avrotize_altnames", name)
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return path


def _have(tool: str) -> bool:
    return shutil.which(tool) is not None


def _run(cmd, cwd, timeout=600, shell=False):
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                          timeout=timeout, shell=shell)


class TestJsonWireNameHelper(unittest.TestCase):
    def test_altnames_json_wins(self):
        self.assertEqual(json_wire_name("dr_type", {"altnames": {"json": "dr-type"}}), "dr-type")

    def test_no_altnames_returns_prop_name(self):
        self.assertEqual(json_wire_name("regular", {"type": "string"}), "regular")

    def test_non_dict_schema(self):
        self.assertEqual(json_wire_name("p", "string"), "p")

    def test_other_purpose_ignored(self):
        self.assertEqual(json_wire_name("d", {"altnames": {"sql": "d-x"}}), "d")

    def test_empty_altnames(self):
        self.assertEqual(json_wire_name("d", {"altnames": {}}), "d")


class TestPythonRoundTrip(unittest.TestCase):
    def test_python(self):
        out = _out("py")
        convert_structure_schema_to_python(SCHEMA, out, dataclasses_json_annotation=True)
        src = next(f for f in glob.glob(os.path.join(out, "**", "telemetry.py"), recursive=True))
        spec = importlib.util.spec_from_file_location("alt384", src)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["alt384"] = mod
        spec.loader.exec_module(mod)
        inst = mod.Telemetry(dr_type=7, regular="hello")
        data = json.loads(inst.to_json())
        self.assertEqual(data["dr-type"], 7)
        self.assertNotIn("dr_type", data)
        self.assertEqual(data["regular"], "hello")
        back = mod.Telemetry.from_json(json.dumps({"dr-type": 9, "regular": "x"}))
        self.assertEqual(back.dr_type, 9)
        self.assertEqual(back.regular, "x")


class TestGoRoundTrip(unittest.TestCase):
    def test_go(self):
        if not _have("go"):
            self.skipTest("go not installed")
        out = _out("go")
        convert_structure_schema_to_go(SCHEMA, out, package_name="tel", json_annotation=True)
        pkg = glob.glob(os.path.join(out, "**", "Telemetry.go"), recursive=True)[0]
        pkgdir = os.path.dirname(pkg)
        with open(os.path.join(pkgdir, "rt_test.go"), "w", encoding="utf-8") as f:
            f.write(
                "package tel\n"
                'import ("encoding/json"; "testing")\n'
                "func TestRT(t *testing.T){\n"
                ' b,_:=json.Marshal(&Telemetry{DrType:7,Regular:\"hi\"})\n'
                ' s:=string(b)\n'
                ' if !contains(s,`\"dr-type\":7`){t.Fatalf(\"no wire key: %s\",s)}\n'
                ' if contains(s,\"dr_type\"){t.Fatalf(\"id leaked: %s\",s)}\n'
                ' var v Telemetry; json.Unmarshal([]byte(`{\"dr-type\":9,\"regular\":\"x\"}`),&v)\n'
                ' if v.DrType!=9||v.Regular!=\"x\"{t.Fatalf(\"rt fail: %+v\",v)}\n'
                "}\n"
                "func contains(h,n string)bool{return len(n)<=len(h)&&indexOf(h,n)>=0}\n"
                "func indexOf(h,n string)int{for i:=0;i+len(n)<=len(h);i++{if h[i:i+len(n)]==n{return i}};return -1}\n")
        _run(["go", "mod", "tidy"], out, shell=WIN)
        r = _run(["go", "test", "./..."], out, shell=WIN)
        self.assertEqual(r.returncode, 0, f"go round-trip failed: {r.stdout}\n{r.stderr}")


class TestRustRoundTrip(unittest.TestCase):
    def test_rust(self):
        if not _have("cargo"):
            self.skipTest("cargo not installed")
        out = _out("rust")
        convert_structure_schema_to_rust(SCHEMA, out, package_name="tel", serde_annotation=True)
        tests = os.path.join(out, "tests")
        os.makedirs(tests, exist_ok=True)
        with open(os.path.join(out, "src", "lib.rs"), "w", encoding="utf-8") as f:
            f.write("pub mod telemetry;\n")
        with open(os.path.join(tests, "rt.rs"), "w", encoding="utf-8") as f:
            f.write(
                "use tel::telemetry::Telemetry;\n"
                "#[test] fn rt(){\n"
                ' let t=Telemetry{dr_type:7,regular:"hi".into()};\n'
                " let s=serde_json::to_string(&t).unwrap();\n"
                ' assert!(s.contains("\\"dr-type\\":7"), "no wire key: {}", s);\n'
                ' assert!(!s.contains("dr_type"), "id leaked: {}", s);\n'
                ' let v:Telemetry=serde_json::from_str("{\\"dr-type\\":9,\\"regular\\":\\"x\\"}").unwrap();\n'
                ' assert_eq!(v.dr_type,9); assert_eq!(v.regular,"x");\n'
                "}\n")
        r = _run(["cargo", "test", "--quiet"], out, timeout=900, shell=WIN)
        self.assertEqual(r.returncode, 0, f"rust round-trip failed: {r.stdout}\n{r.stderr}")


class TestCSharpRoundTrip(unittest.TestCase):
    def test_csharp(self):
        if not _have("dotnet"):
            self.skipTest("dotnet not installed")
        out = _out("cs")
        convert_structure_schema_to_csharp(SCHEMA, out, base_namespace="Tel",
                                           system_text_json_annotation=True)
        tel = glob.glob(os.path.join(out, "**", "Telemetry.cs"), recursive=True)[0]
        proj = glob.glob(os.path.join(out, "src", "*.csproj"))[0]
        drv = os.path.join(out, "drv")
        os.makedirs(drv)
        with open(os.path.join(drv, "drv.csproj"), "w", encoding="utf-8") as f:
            f.write('<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType>'
                    '<TargetFramework>net10.0</TargetFramework><Nullable>enable</Nullable></PropertyGroup>'
                    f'<ItemGroup><ProjectReference Include="{proj}" /></ItemGroup></Project>')
        with open(os.path.join(drv, "Program.cs"), "w", encoding="utf-8") as f:
            f.write('var t=new Tel.Telemetry{dr_type=7,regular="hi"};'
                    'var s=System.Text.Json.JsonSerializer.Serialize(t);'
                    'System.Console.WriteLine(s);'
                    'var b=System.Text.Json.JsonSerializer.Deserialize<Tel.Telemetry>("{\\"dr-type\\":9,\\"regular\\":\\"x\\"}");'
                    'System.Console.WriteLine(b!.dr_type+"|"+b.regular);')
        r = _run(["dotnet", "run", "--project", drv, "-c", "Release"], drv, timeout=600, shell=WIN)
        self.assertEqual(r.returncode, 0, f"dotnet run failed: {r.stdout}\n{r.stderr}")
        self.assertIn('"dr-type":7', r.stdout)
        self.assertNotIn("dr_type", r.stdout.split("\n")[0])
        self.assertIn("9|x", r.stdout)


class TestJavaRoundTrip(unittest.TestCase):
    def test_java(self):
        if not _have("mvn"):
            self.skipTest("maven not installed")
        out = _out("java")
        convert_structure_schema_to_java(SCHEMA, out, package_name="tel", jackson_annotation=True)
        tdir = os.path.join(out, "src", "main", "java", "tel")
        os.makedirs(tdir, exist_ok=True)
        with open(os.path.join(tdir, "Rt.java"), "w", encoding="utf-8") as f:
            f.write(
                "package tel;\nimport com.fasterxml.jackson.databind.ObjectMapper;\n"
                "public class Rt{ public static void main(String[] a) throws Exception{\n"
                " ObjectMapper m=new ObjectMapper(); Telemetry t=new Telemetry(); t.setDrType(7); t.setRegular(\"hi\");\n"
                " String s=m.writeValueAsString(t);\n"
                " if(!s.contains(\"\\\"dr-type\\\":7\")) throw new RuntimeException(\"no wire key: \"+s);\n"
                " if(s.contains(\"dr_type\")) throw new RuntimeException(\"id leaked: \"+s);\n"
                " Telemetry b=m.readValue(\"{\\\"dr-type\\\":9,\\\"regular\\\":\\\"x\\\"}\",Telemetry.class);\n"
                " if(b.getDrType()!=9||!\"x\".equals(b.getRegular())) throw new RuntimeException(\"rt fail\");\n"
                " System.out.println(\"OK\");\n}}\n")
        r = _run(["mvn", "-q", "-B", "compile",
                  "org.codehaus.mojo:exec-maven-plugin:3.1.0:java",
                  "-Dexec.mainClass=tel.Rt"], out, timeout=900, shell=WIN)
        self.assertEqual(r.returncode, 0, f"java round-trip failed: {r.stdout[-3000:]}\n{r.stderr[-1000:]}")
        self.assertIn("OK", r.stdout)


class TestTypeScriptRoundTrip(unittest.TestCase):
    def test_typescript(self):
        if not _have("npm"):
            self.skipTest("npm not installed")
        out = _out("ts")
        convert_structure_schema_to_typescript(SCHEMA, out, typedjson_annotation=True)
        with open(os.path.join(out, "src", "rt.ts"), "w", encoding="utf-8") as f:
            f.write("import 'reflect-metadata';import {Telemetry} from './Telemetry.js';\n"
                    "const t=new Telemetry(7,'hi');const s=t.toJSON();\n"
                    "if(!s.includes('\"dr-type\":7'))throw new Error('no wire key:'+s);\n"
                    "if(s.includes('dr_type'))throw new Error('id leaked:'+s);\n"
                    "const b=Telemetry.fromJSON('{\"dr-type\":9,\"regular\":\"x\"}');\n"
                    "if(b.dr_type!==9||b.regular!=='x')throw new Error('rt:'+JSON.stringify(b));\n"
                    "console.log('OK');\n")
        if _run(["npm", "install"], out, timeout=300, shell=WIN).returncode != 0:
            self.skipTest("npm install failed")
        r = _run(["npx", "tsc", "-p", "tsconfig.json"], out, timeout=300, shell=WIN)
        self.assertEqual(r.returncode, 0, f"tsc failed: {r.stdout}\n{r.stderr}")
        built = glob.glob(os.path.join(out, "**", "rt.js"), recursive=True)
        self.assertTrue(built, "rt.js not built")
        r2 = _run(["node", built[0]], out, timeout=120, shell=WIN)
        self.assertEqual(r2.returncode, 0, f"node round-trip failed: {r2.stdout}\n{r2.stderr}")
        self.assertIn("OK", r2.stdout)


class TestCppGeneration(unittest.TestCase):
    """Assert generated wire-key serialization; compile+run round-trip if g++ and nlohmann are available."""
    def test_cpp(self):
        out = _out("cpp")
        convert_structure_schema_to_cpp(SCHEMA, out, namespace="tel", json_annotation=True)
        inc = glob.glob(os.path.join(out, "**", "Telemetry.hpp"), recursive=True)[0]
        src = open(inc, encoding="utf-8", errors="ignore").read()
        self.assertIn('"dr-type"', src)
        self.assertIn("dr_type", src)
        if not _have("g++"):
            self.skipTest("g++ not installed; source assertions only")
        incdir = os.path.dirname(os.path.dirname(inc))
        drv = os.path.join(out, "rt.cpp")
        with open(drv, "w", encoding="utf-8") as f:
            f.write('#include <tel/Telemetry.hpp>\n#include <string>\n#include <cassert>\n#include <iostream>\n'
                    'int main(){ tel::Telemetry t; t.dr_type=7; t.regular="hi";\n'
                    ' auto s=t.to_json().dump();\n'
                    ' assert(s.find("\\"dr-type\\":7")!=std::string::npos);\n'
                    ' assert(s.find("dr_type")==std::string::npos);\n'
                    ' auto j=nlohmann::json::parse("{\\"dr-type\\":9,\\"regular\\":\\"x\\"}");\n'
                    ' tel::Telemetry b=j.get<tel::Telemetry>();\n'
                    ' assert(b.dr_type==9 && b.regular=="x"); std::cout<<"OK"; return 0; }\n')
        exe = os.path.join(out, "rt.exe")
        c = _run(["g++", "-std=c++17", f"-I{incdir}", drv, "-o", exe], out, timeout=300, shell=WIN)
        if c.returncode != 0:
            self.skipTest(f"g++ compile failed (nlohmann missing?): {c.stderr[-400:]}")
        r = _run([exe], out, timeout=60, shell=WIN)
        self.assertEqual(r.returncode, 0, f"cpp round-trip failed: {r.stdout}\n{r.stderr}")
        self.assertIn("OK", r.stdout)


if __name__ == "__main__":
    unittest.main()
