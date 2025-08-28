# 本文件是控制器 / 路由 (Controller)。数据库读写全再db.py里，通过from . import db调用，保持分层

''' FastAPI：创建 Web 应用实例（app = FastAPI(...)）
    Request：用于把请求对象传给模板（模板里常需要 request）。
    Form：用于从 HTML 表单里取字段（见下面的 reserve_add 路由）。
'''
from fastapi import FastAPI, Request, Form, Query
'''
    HTMLResponse：声明这个路由返回的是 HTML（不是 JSON）。
    RedirectResponse：操作成功后重定向（比如提交表单后回到首页）。
'''
from fastapi.responses import HTMLResponse, RedirectResponse
# status：状态码枚举，这里用 HTTP_303_SEE_OTHER。
from starlette import status
# Jinja2Templates：配置 Jinja2 模板目录，让你可以 TemplateResponse("index.html", {...})。
from fastapi.templating import Jinja2Templates
from typing import Dict, Optional, List

from . import db

app = FastAPI(title="出让库软件（雏形）")
templates = Jinja2Templates(directory="app/templates")



# 字段 -> 中文名（用于详情页展示）
FIELD_LABELS = {
    "id": "ID",
    "name": "名称",
    "region": "地州",
    "mineral_type": "矿种",
    "area": "面积(km²)",
    "quantity": "资源量",
    "recommendations": "分类建议",
    "coordinates": "坐标",
    "transfer_conditions": "是否满足出让条件",
    "announcement_date": "发布出让公告时间",
    "annual_transfer_batch": "年度出让批次",
    "proj_source": "项目来源",
    "start_price": "起始价(万元)",
    "transaction_date": "成交日期",
    "transaction_price": "成交价(万元)",
    "payable_price": "应付金额(万元)",
    "success_bidder": "竞得人",
    "contact_name": "联系人",
    "social_credit_code": "社会信用代码",
    "contact_number": "联系电话",
    "company_address": "公司地址",
    "transfer_authority": "出让权限",
    "contract_number": "合同编号",
    "contract_signing_date": "合同签订日期",
    "payment_deadline": "付款截止日期",
    "actual_payment_date": "实际付款日期",
}

TABLE_LABELS = {
    "reserve": "储备库",
    "offering": "出让库",
    "deal": "成交库",
}
templates.env.globals["FIELD_LABELS"] = FIELD_LABELS
templates.env.globals["TABLE_LABELS"] = TABLE_LABELS
# FastAPI 的启动钩子
@app.on_event("startup")
def on_startup():
    db.init_db()
    
@app.get("/", response_class=HTMLResponse)
def home_cards(request: Request):
    # 首页只渲染卡片
    return templates.TemplateResponse("home.html", {"request": request})

# 列表页接收删除提示参数
@app.get("/view/{table}", response_class=HTMLResponse)
def view_table(request: Request, table: str, del_: int | None = Query(default=None)):
    cols = ["id", "name", "mineral_type", "region", "announcement_date"]
    reserve_rows = db.list_table("reserve", cols=cols)
    offering_rows = db.list_table("offering", cols=cols)
    deal_rows = db.list_table("deal", cols=cols)
    active_table = table if table in {"reserve", "offering", "deal"} else "reserve"
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "reserve_rows": reserve_rows,
            "offering_rows": offering_rows,
            "deal_rows": deal_rows,
            "active_table": active_table,
            "deleted": bool(del_),
        },
    )


# 读三张表，并渲染HTML
# 访问 GET /（首页） 告诉 FastAPI 这是HTML页面

# 详情页（查看 / 编辑）
@app.get("/detail/{table}/{id_}", response_class=HTMLResponse)
def detail_page(
    request: Request, 
    table: str, 
    id_: int,
    edit: int | None = Query(default=None),
    ok: int | None = Query(default=None),
    err: int | None = Query(default=None),
    delete: int | None = Query(default=None),   # <- 新增
):
    record = db.get_record(table, id_, cols=db.ALL_COLUMNS)
    items = []
    for col in db.ALL_COLUMNS:
        label = FIELD_LABELS.get(col, col)
        items.append((label, record.get(col, "")))
    return templates.TemplateResponse(
        "detail.html",
        {
            "request": request,
            "table": table,
            "table_label": TABLE_LABELS.get(table, table),
            "items": items,
            "id_": id_,
            "record": record,
            "edit": bool(edit),
            "ok": ok,
            "err": err,
            "delete_confirm": bool(delete),   # <- 新增：模板用它渲染确认表单
        },
    )

# 删除记录
@app.post("/detail/{table}/{id_}/delete")
def detail_delete(table: str, id_: int):
    try:
        rc = db.delete_record(table, id_)
        if rc == 1:
            # ✅ 删除成功后回到该库列表，并带提示
            return RedirectResponse(
                url=f"/view/{table}?del_=1",
                status_code=status.HTTP_303_SEE_OTHER
            )
        else:
            return RedirectResponse(
                url=f"/detail/{table}/{id_}?err=1",
                status_code=status.HTTP_303_SEE_OTHER
            )
    except Exception as e:
        print("delete_record error:", e)
        return RedirectResponse(
            url=f"/detail/{table}/{id_}?err=1",
            status_code=status.HTTP_303_SEE_OTHER
        )

    
# 保存更新
@app.post("/detail/{table}/{id_}/update")
async def detail_update(request: Request, table: str, id_: int):
    form = await request.form()
    # 把空串统一转 None，保持与新增一致
    data = {}
    for col in db.ALL_COLUMNS:
        if col == "id":
            continue
        v = form.get(col)
        if isinstance(v, str):
            v = v.strip()
        data[col] = (None if v in (None, "") else v)

    try:
        rc = db.update_full_record(table, id_, data)
        if rc == 1:
            return RedirectResponse(
                f"/detail/{table}/{id_}?ok=1", status_code=status.HTTP_303_SEE_OTHER
            )
        else:
            # 没找到 id
            return RedirectResponse(
                f"/detail/{table}/{id_}?err=1", status_code=status.HTTP_303_SEE_OTHER
            )
    except Exception as e:
        print("update_full_record error:", e)
        return RedirectResponse(
            f"/detail/{table}/{id_}?err=1", status_code=status.HTTP_303_SEE_OTHER
        )
    
# ---------- 新增：展示“新增记录”页面 ----------
@app.get("/add", response_class=HTMLResponse)
def add_page(request: Request, ok: int | None = Query(default=None), err: str | None = Query(default=None)):
    return templates.TemplateResponse(
        "add.html",
        {"request": request, "ok": ok, "err": err}
    )

# 这个接口只接受POST(来自表单<form method="post">提交)请求   
@app.post("/add")
async def add_submit(request: Request):
    form = await request.form()
    table = (form.get("table") or "").strip()
    # 将表单转为普通 dict，并清理空串 -> None
    data = {}
    for k, v in form.items():
        if k == "table":
            continue
        v = v.strip() if isinstance(v, str) else v
        data[k] = (None if v == "" else v)

    # 基本校验
    if not data.get("id") or not data.get("name"):
        return RedirectResponse("/add?err=1", status_code=status.HTTP_303_SEE_OTHER)

    try:
        # 允许数字字段直接传字符串（SQLite 会宽松处理）
        db.add_full_record(table, data)
        return RedirectResponse("/add?ok=1", status_code=status.HTTP_303_SEE_OTHER)
    except Exception as e:
        print("add_full_record error:", e)
        return RedirectResponse("/add?err=1", status_code=status.HTTP_303_SEE_OTHER)

# 储备库 → 出让库
@app.post("/move/reserve/{rid}")
def move_reserve_to_offering(rid: int):
    try:
        db.move_reserve_to_offering(rid)
    except Exception as e:
        # 这里简单处理；后面我们会加“闪存提示/错误提示”
        print("move_reserve_to_offering error:", e)
    return RedirectResponse("/view/reserve", status_code=status.HTTP_303_SEE_OTHER)

# 出让库 → 成交库
@app.post("/move/offering/{oid}")
def move_offering_to_deal(oid: int):
    try:
        db.move_offering_to_deal(oid)
    except Exception as e:
        print("move_offering_to_deal error:", e)
    return RedirectResponse("/view/offering", status_code=status.HTTP_303_SEE_OTHER)