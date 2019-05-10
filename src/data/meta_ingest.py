# meta_ingest.py

import pandas as pd

def gen_meta_nodelist(filepath):
    nl = pd.read_excel(filepath, sheetname=0, 
                        usecols=["gameid", "side", "position", "champion", 
                                 "result", "k", "d", "a"])
    # get rid of team observations
    nl = nl[nl["position"] != "Team"]
    return nl

def gen_meta_edgelist(filepath):
    import itertools as itr
    # read in the data
    df = pd.read_excel(filepath, sheetname=0,
                       usecols=["gameid", "side", "position", "champion", 
                                "ban1", "ban2", "ban3", "ban4", "ban5"])
    # get rid of team observations
    df = df[df["position"] != "Team"]
    
    # initialize empty data frames
    pw = pd.DataFrame()
    ba = pd.DataFrame()

    def gen_pw(group_df):
        new = pd.DataFrame()
        new["pick_combos"] = list(itr.combinations(group_df.champion, 2))
        
        new[["champ_a", "champ_b"]] = pd.DataFrame(new.pick_combos.values.tolist(), 
                                                        index=new.index)
        new = new.drop(["pick_combos"], axis = 1)
        new["link_type"] = "pick"
        new["gameid"] = group_df.gameid.values[0]
        return new
    
    def gen_ba(group_df):
        new = pd.DataFrame()
        # loop through ban columns
        ban_combos = []
        for ban in range(1,6):
            ban_combos.extend(list(zip(group_df.champion.values, eval("group_df.ban%s.values" % ban))))
        new["ban_combos"] = ban_combos
        new[["champ_a", "champ_b"]] = pd.DataFrame(new.ban_combos.values.tolist(), 
                                                        index=new.index)
        new = new.drop(["ban_combos"], axis = 1)
        new["link_type"] = "ban"
        new["gameid"] = group_df.gameid.values[0]
        return new
    
    # loop through all the groups
    for name, group in df.groupby(["gameid", "side"], as_index=False):
        pw = pw.append(gen_pw(group))
        # grab banned against
        ba = ba.append(gen_ba(group))

    return pw.append(ba)

def gen_meta_info(filepath):
    meta_info = pd.read_excel(filepath, sheetname=0,
                              usecols=["gameid", "league", "split", "date", 
                                       "week", "patchno"])

    return meta_info.drop_duplicates()