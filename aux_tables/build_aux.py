import csv, sys, collections
SRC = sys.argv[1]
DST = sys.argv[2]
PROV = {
 "01":("Araba/Álava","País Vasco"),"02":("Albacete","Castilla-La Mancha"),
 "03":("Alicante/Alacant","Comunitat Valenciana"),"04":("Almería","Andalucía"),
 "05":("Ávila","Castilla y León"),"06":("Badajoz","Extremadura"),
 "07":("Illes Balears","Illes Balears"),"08":("Barcelona","Cataluña"),
 "09":("Burgos","Castilla y León"),"10":("Cáceres","Extremadura"),
 "11":("Cádiz","Andalucía"),"12":("Castellón/Castelló","Comunitat Valenciana"),
 "13":("Ciudad Real","Castilla-La Mancha"),"14":("Córdoba","Andalucía"),
 "15":("A Coruña","Galicia"),"16":("Cuenca","Castilla-La Mancha"),
 "17":("Girona","Cataluña"),"18":("Granada","Andalucía"),
 "19":("Guadalajara","Castilla-La Mancha"),"20":("Gipuzkoa","País Vasco"),
 "21":("Huelva","Andalucía"),"22":("Huesca","Aragón"),
 "23":("Jaén","Andalucía"),"24":("León","Castilla y León"),
 "25":("Lleida","Cataluña"),"26":("La Rioja","La Rioja"),
 "27":("Lugo","Galicia"),"28":("Madrid","Comunidad de Madrid"),
 "29":("Málaga","Andalucía"),"30":("Murcia","Región de Murcia"),
 "31":("Navarra","Comunidad Foral de Navarra"),"32":("Ourense","Galicia"),
 "33":("Asturias","Principado de Asturias"),"34":("Palencia","Castilla y León"),
 "35":("Las Palmas","Canarias"),"36":("Pontevedra","Galicia"),
 "37":("Salamanca","Castilla y León"),"38":("Santa Cruz de Tenerife","Canarias"),
 "39":("Cantabria","Cantabria"),"40":("Segovia","Castilla y León"),
 "41":("Sevilla","Andalucía"),"42":("Soria","Castilla y León"),
 "43":("Tarragona","Cataluña"),"44":("Teruel","Aragón"),
 "45":("Toledo","Castilla-La Mancha"),"46":("Valencia/València","Comunitat Valenciana"),
 "47":("Valladolid","Castilla y León"),"48":("Bizkaia","País Vasco"),
 "49":("Zamora","Castilla y León"),"50":("Zaragoza","Aragón"),
 "51":("Ceuta","Ceuta"),"52":("Melilla","Melilla"),
}
rows, bad, ids = [], [], collections.Counter()
with open(SRC, newline="", encoding="utf-8") as f:
    rd = csv.reader(f, delimiter="|", quoting=csv.QUOTE_NONE)
    header = next(rd)
    assert header == ["", "ID", "name"], header
    for r in rd:
        if not r: continue
        assert len(r) == 3, r
        _, zid, name = r
        cod = zid[:2]
        ids[zid] += 1
        if cod not in PROV:
            bad.append(zid); continue
        prov, ccaa = PROV[cod]
        rows.append((zid, name, cod, prov, ccaa))
with open(DST, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, delimiter="|", lineterminator="\n", quoting=csv.QUOTE_NONE, escapechar="\\")
    w.writerow(["ID","name","cod_provincia","provincia","ccaa"])
    w.writerows(rows)
print("filas:", len(rows), "| sin provincia:", len(bad), bad[:10])
print("ids duplicados:", [k for k,v in ids.items() if v>1][:10])
print("provincias distintas:", len({r[2] for r in rows}), "| CCAA distintas:", len({r[4] for r in rows}))
print("nombres con '|' o comillas:", sum(1 for r in rows if '|' in r[1] or '"' in r[1]))
