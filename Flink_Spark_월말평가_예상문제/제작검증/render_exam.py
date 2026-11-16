from pathlib import Path
import html
import re
import subprocess
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from pypdf import PdfReader
from PIL import Image

base = Path(__file__).resolve().parents[1]
out = base / "output" / "pdf"
out.mkdir(parents=True, exist_ok=True)
qa = base / "제작검증" / "render"
qa.mkdir(exist_ok=True)
target = out / "Flink_Spark_월말평가_문제지.pdf"
pdfmetrics.registerFont(TTFont("Malgun", "C:/Windows/Fonts/malgun.ttf"))
pdfmetrics.registerFont(TTFont("MalgunB", "C:/Windows/Fonts/malgunbd.ttf"))
pdfmetrics.registerFontFamily("Malgun", normal="Malgun", bold="MalgunB")
navy = colors.HexColor("#16283E")
teal = colors.HexColor("#036862")
styles = {
    "title": ParagraphStyle("title",fontName="MalgunB",fontSize=28,leading=38,
                            textColor=navy,spaceAfter=19,wordWrap="CJK"),
    "head": ParagraphStyle("head",fontName="MalgunB",fontSize=15,leading=22,
                           textColor=teal,spaceAfter=10,wordWrap="CJK"),
    "body": ParagraphStyle("body",fontName="Malgun",fontSize=10.5,leading=16.5,
                           textColor=navy,spaceAfter=8,wordWrap="CJK"),
    "code": ParagraphStyle("code",fontName="Malgun",fontSize=9.2,leading=14,
                           textColor=navy,spaceAfter=11,wordWrap="CJK",
                           backColor=colors.HexColor("#EDF4F8"),borderPadding=6),
}
def para(text, kind="body"):
    text = html.escape(text)
    text = re.sub(r"\*\*(.+?)\*\*",lambda m:"<b>"+m[1]+"</b>",text)
    text = re.sub(chr(96)+r"(.+?)"+chr(96),lambda m:'<font color="#036862">'+m[1]+'</font>',text)
    return Paragraph(text,styles[kind])
def footer(c,doc):
    c.setStrokeColor(colors.HexColor("#CFDFE6"))
    c.line(44,42,A4[0]-44,42)
    c.setFillColor(colors.HexColor("#435971"))
    c.setFont("Malgun",8)
    c.drawString(44,28,"SSAFY 월말평가 대비 모의시험  /  Flink + Spark")
    c.drawRightString(A4[0]-44,28,str(doc.page))
text=(base/"응시용/problem/문제지.md").read_text(encoding="utf-8")
parts=re.split(r"^## ",text,flags=re.M)
story=[Spacer(1,22),para("데이터 트랙  /  5회차 월말평가 대비","head"),
       para("Flink + Spark 코드베이스 모의시험","title")]
for line in parts[0].splitlines()[1:]:
    if line.strip(): story.append(para(line))
story += [Spacer(1,16),para("실행 환경","head"),
          para("JDK 17과 Python 3.10 또는 3.11 기준입니다. problem/README.md의 버전 및 로컬 실행 안내를 확인하세요."),
          para("python -m pip install -r requirements.txt","code"),
          para("수정 및 제출","head"),
          para("번호 빈칸, TODO 및 지정한 오류 줄을 수정합니다. 문제 1~7은 명령과 결과를 한 화면에 담아 각 폴더에 PNG로 저장합니다."),
          para("문제 8은 problem8.txt에 작성합니다. 제출 전 파일과 프로그램의 정상 실행을 확인하세요.")]
for i,part in enumerate(parts[1:]):
    story.append(PageBreak() if i%2==0 else Spacer(1,18))
    title,*body=part.splitlines()
    story.append(para(title,"head"))
    code=None
    for line in body:
        if line.startswith(chr(96)*3):
            if code is None: code=[]
            else:
                story.append(Paragraph("<br/>".join(html.escape(x) for x in code),styles["code"]))
                code=None
        elif code is not None: code.append(line)
        elif line.strip(): story.append(para("• "+line[2:] if line.startswith("- ") else line))
doc=SimpleDocTemplate(str(target),pagesize=A4,leftMargin=44,rightMargin=44,topMargin=43,
                      bottomMargin=57,title="Flink + Spark 월말평가 모의시험")
doc.build(story,onFirstPage=footer,onLaterPages=footer)
reader=PdfReader(target)
content="\n".join(p.extract_text() for p in reader.pages)
assert all(f"문제 {n:02d}." in content for n in range(1,9))
assert len(reader.pages)==5, len(reader.pages)
pop="C:/Users/82109/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/Library/bin/pdftoppm.exe"
subprocess.run([pop,"-scale-to","1250","-png",str(target),str(qa/"exam")],check=True)
pages=sorted(qa.glob("exam-*.png"))
assert len(pages)==len(reader.pages)
sheet=Image.new("RGB",(1500,450),"#dce5ee")
for i,p in enumerate(pages):
    with Image.open(p) as img:
        img.thumbnail((294,440))
        sheet.paste(img,(i*300+3,3))
sheet.save(qa/"overview.png")
print(f"PDF pages rendered: {len(pages)}")
