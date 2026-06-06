import requests , json , binascii , time , urllib3 , base64 , datetime , re ,socket , threading , random , os , asyncio
from protobuf_decoder.protobuf_decoder import Parser
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad , unpad
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

Key , Iv = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56]) , bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])

async def EnC_AEs(HeX):
    cipher = AES.new(Key , AES.MODE_CBC , Iv)
    return cipher.encrypt(pad(bytes.fromhex(HeX), AES.block_size)).hex()
    
async def DEc_AEs(HeX):
    cipher = AES.new(Key , AES.MODE_CBC , Iv)
    return unpad(cipher.decrypt(bytes.fromhex(HeX)), AES.block_size).hex()
    
async def EnC_PacKeT(HeX , K , V): 
    return AES.new(K , AES.MODE_CBC , V).encrypt(pad(bytes.fromhex(HeX) ,16)).hex()
    
async def DEc_PacKeT(HeX , K , V):
    return unpad(AES.new(K , AES.MODE_CBC , V).decrypt(bytes.fromhex(HeX)) , 16).hex()  

async def EnC_Uid(H , Tp):
    e , H = [] , int(H)
    while H:
        e.append((H & 0x7F) | (0x80 if H > 0x7F else 0)) ; H >>= 7
    return bytes(e).hex() if Tp == 'Uid' else None

async def EnC_Vr(N):
    if N < 0: ''
    H = []
    while True:
        BesTo = N & 0x7F ; N >>= 7
        if N: BesTo |= 0x80
        H.append(BesTo)
        if not N: break
    return bytes(H)
    
def DEc_Uid(H):
    n = s = 0
    for b in bytes.fromhex(H):
        n |= (b & 0x7F) << s
        if not b & 0x80: break
        s += 7
    return n
    
async def CrEaTe_VarianT(field_number, value):
    field_header = (field_number << 3) | 0
    return await EnC_Vr(field_header) + await EnC_Vr(value)

async def CrEaTe_LenGTh(field_number, value):
    field_header = (field_number << 3) | 2
    encoded_value = value.encode() if isinstance(value, str) else value
    return await EnC_Vr(field_header) + await EnC_Vr(len(encoded_value)) + encoded_value

async def CrEaTe_ProTo(fields):
    packet = bytearray()
    for field, value in fields.items():
        if isinstance(value, dict):
            nested_packet = await CrEaTe_ProTo(value)  
            packet.extend(await CrEaTe_LenGTh(field, nested_packet))
        elif isinstance(value, int):
            packet.extend(await CrEaTe_VarianT(field, value))
        elif isinstance(value, str) or isinstance(value, bytes):
            packet.extend(await CrEaTe_LenGTh(field, value))
    return packet
    
async def DecodE_HeX(H):
    R = hex(H) 
    F = str(R)[2:]
    if len(F) == 1: F = "0" + F ; return F
    else: return F

async def Fix_PackEt(parsed_results):
    result_dict = {}
    for result in parsed_results:
        field_data = {}
        field_data['wire_type'] = result.wire_type
        if result.wire_type == "varint":
            field_data['data'] = result.data
        if result.wire_type == "string":
            field_data['data'] = result.data
        if result.wire_type == "bytes":
            field_data['data'] = result.data
        elif result.wire_type == 'length_delimited':
            field_data["data"] = await Fix_PackEt(result.data.results)
        result_dict[result.field] = field_data
    return result_dict

async def DeCode_PackEt(input_text):
    try:
        parsed_results = Parser().parse(input_text)
        parsed_results_objects = parsed_results
        parsed_results_dict = await Fix_PackEt(parsed_results_objects)
        json_data = json.dumps(parsed_results_dict)
        return json_data
    except Exception as e:
        print(f"error {e}")
        return None

async def encrypt_packet(plain_hex, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plain_bytes = bytes.fromhex(plain_hex)
    padded = pad(plain_bytes, AES.block_size)
    encrypted = cipher.encrypt(padded)
    return encrypted.hex()
    
async def GeneRaTePk(packet_hex, packet_type, K, V):
    try:
        encrypted_packet = await encrypt_packet(packet_hex, K, V)
        packet_length = len(encrypted_packet) // 2
        packet_length_hex = await base_to_hex(packet_length)
        
        if len(packet_length_hex) == 2:
            final_packet = f"{packet_type}000000{packet_length_hex}{encrypted_packet}"
        elif len(packet_length_hex) == 3:
            final_packet = f"{packet_type}00000{packet_length_hex}{encrypted_packet}"
        elif len(packet_length_hex) == 4:
            final_packet = f"{packet_type}0000{packet_length_hex}{encrypted_packet}"
        elif len(packet_length_hex) == 5:
            final_packet = f"{packet_type}000{packet_length_hex}{encrypted_packet}"
        else:
            final_packet = f"{packet_type}00{packet_length_hex}{encrypted_packet}"
            
        return bytes.fromhex(final_packet)
    except Exception as e:
        logging.error(f"Error in GeneRaTePk: {e}")
        return None
        
async def base_to_hex(timestamp):
    timestamp_result = hex(timestamp)
    result = str(timestamp_result)[2:]
    if len(result) == 1:
        result = "0" + result
    return result
                      
def fixnum(num):
    num_str = str(num)
    return "[C]" + "[C]".join(num_str) + "[C]"
    
async def Ua():
    versions = [
        '4.0.18P6', '4.0.19P7', '4.0.20P1', '4.1.0P3', '4.1.5P2', '4.2.1P8',
        '4.2.3P1', '5.0.1B2', '5.0.2P4', '5.1.0P1', '5.2.0B1', '5.2.5P3',
        '5.3.0B1', '5.3.2P2', '5.4.0P1', '5.4.3B2', '5.5.0P1', '5.5.2P3'
    ]
    models = [
        'SM-A125F', 'SM-A225F', 'SM-A325M', 'SM-A515F', 'SM-A725F', 'SM-M215F', 'SM-M325FV',
        'Redmi 9A', 'Redmi 9C', 'POCO M3', 'POCO M4 Pro', 'RMX2185', 'RMX3085',
        'moto g(9) play', 'CPH2239', 'V2027', 'OnePlus Nord', 'ASUS_Z01QD',
    ]
    android_versions = ['9', '10', '11', '12', '13', '14']
    languages = ['en-US', 'es-MX', 'pt-BR', 'id-ID', 'ru-RU', 'hi-IN']
    countries = ['USA', 'MEX', 'BRA', 'IDN', 'RUS', 'IND']
    version = random.choice(versions)
    model = random.choice(models)
    android = random.choice(android_versions)
    lang = random.choice(languages)
    country = random.choice(countries)
    return f"GarenaMSDK/{version}({model};Android {android};{lang};{country};)"
    
def Uaa():
    versions = [
        '4.0.18P6', '4.0.19P7', '4.0.20P1', '4.1.0P3', '4.1.5P2', '4.2.1P8',
        '4.2.3P1', '5.0.1B2', '5.0.2P4', '5.1.0P1', '5.2.0B1', '5.2.5P3',
        '5.3.0B1', '5.3.2P2', '5.4.0P1', '5.4.3B2', '5.5.0P1', '5.5.2P3'
    ]
    models = [
        'SM-A125F', 'SM-A225F', 'SM-A325M', 'SM-A515F', 'SM-A725F', 'SM-M215F', 'SM-M325FV',
        'Redmi 9A', 'Redmi 9C', 'POCO M3', 'POCO M4 Pro', 'RMX2185', 'RMX3085',
        'moto g(9) play', 'CPH2239', 'V2027', 'OnePlus Nord', 'ASUS_Z01QD',
    ]
    android_versions = ['9', '10', '11', '12', '13', '14']
    languages = ['en-US', 'es-MX', 'pt-BR', 'id-ID', 'ru-RU', 'hi-IN']
    countries = ['USA', 'MEX', 'BRA', 'IDN', 'RUS', 'IND']
    version = random.choice(versions)
    model = random.choice(models)
    android = random.choice(android_versions)
    lang = random.choice(languages)
    country = random.choice(countries)
    return f"GarenaMSDK/{version}({model};Android {android};{lang};{country};)"
    
def ArA_CoLor():
    Tp = ["32CD32" , "00BFFF" , "00FA9A" , "90EE90" , "FF4500" , "FF6347" , "FF69B4" , "FF8C00" , "FF6347" , "FFD700" , "FFDAB9" , "F0F0F0" , "F0E68C" , "D3D3D3" , "A9A9A9" , "D2691E" , "CD853F" , "BC8F8F" , "6A5ACD" , "483D8B" , "4682B4", "9370DB" , "C71585" , "FF8C00" , "FFA07A"]
    return random.choice(Tp)
    
def get_random_avatar():
    avatar_list = [
        902000204, 902000191, 902038023, 902031017,
        902030016, 902039014, 902000063, 902052025,
        902052007, 902052026, 902052006, 902052010,
        902000281, 902000345, 902034018, 902034019
    ]
    return random.choice(avatar_list)
   
def get_random_colour():
    colors = [
        "[FF0000]", "[00FF00]", "[0000FF]", "[FFFF00]", "[FF00FF]", "[00FFFF]", "[FFFFFF]", "[FFA500]",
        "[A52A2A]", "[800080]", "[000000]", "[808080]", "[C0C0C0]", "[FFC0CB]", "[FFD700]", "[ADD8E6]",
        "[90EE90]", "[D2691E]", "[DC143C]", "[00CED1]", "[9400D3]", "[F08080]", "[20B2AA]", "[FF1493]",
        "[7CFC00]", "[B22222]", "[FF4500]", "[DAA520]", "[00BFFF]", "[00FF7F]", "[4682B4]", "[6495ED]",
        "[5F9EA0]", "[DDA0DD]", "[E6E6FA]", "[B0C4DE]", "[556B2F]", "[8FBC8F]", "[2E8B57]", "[3CB371]",
        "[6B8E23]", "[808000]", "[B8860B]", "[CD5C5C]", "[8B0000]", "[FF6347]", "[FF8C00]", "[BDB76B]",
        "[9932CC]", "[8A2BE2]", "[4B0082]", "[6A5ACD]", "[7B68EE]", "[4169E1]", "[1E90FF]", "[191970]",
        "[00008B]", "[000080]", "[008080]", "[008B8B]", "[B0E0E6]", "[AFEEEE]", "[E0FFFF]", "[F5F5DC]",
        "[FAEBD7]"
    ]
    return random.choice(colors)
    
async def xSEndMsg(Msg, Tp, Tp2, id, K, V, region="IND"):
    feilds = {
        18: 18,
        1: 1,
        2: {
            1: id,
            2: int(Tp2),
            3: Tp,
            4: Msg,
            5: int(time.time()),
            7: 2,
        }
    }

    feilds[2][9] = {
        1: "xBesTo - C4",
        2: int(get_random_avatar()),
        3: 901027033,
        4: 228,
        5: 1001000001,
        8: "xBesTo - C4",
        10: 11,
        11: 1,
        12: 0,
        13: {1: 2},
        14: {
            1: id,
            2: 8,
            3: b"\x0f\x06\x15\x08\x0a\x0b\x13\x0c\x11\x04\x0e\x14\x07\x02\x01\x05\x10\x03\x0d\x12"
        }
    }

    feilds[2][10] = region.lower()

    feilds[2][13] = {2: 1, 3: 1}

    feilds[2][14] = {
        1: {
            1: 1,
            2: 1,
            3: random.randint(1, 62),
            4: 1,
            5: int(time.time()),
            6: region
        }
    }

    Pk = (await CrEaTe_ProTo(feilds)).hex()
    return await GeneRaTePk(Pk, '1201', K, V)
    
async def xSEndMsgsQ(Msg, id, K, V, region="IND"):
    avatar = get_random_avatar()
    timestamp = int(time.time())
    
    fields = {
        1: id, 
        2: id, 
        4: Msg, 
        5: 1756580149, 
        7: 2, 
        8: 904990072, 
        9: {
            1: "xBe4!sTo - C4", 
            2: avatar, 
            4: 329, 
            5: 1001000001, 
            8: "xBe4!sTo - C4", 
            10: 11, 
            11: 1,
            13: {1: 2}, 
            14: {
                1: 1158053040, 
                2: 8, 
                3: b"\x10\x15\x08\x0A\x0B\x15\x0C\x0F\x11\x04\x07\x02\x03\x0D\x0E\x12\x01\x05\x06"
            }
        }, 
        10: "en", 
        13: {2: 2, 3: 1},
        14: {
            1: {
                1: 1,
                2: 1,
                3: random.randint(1, 62),
                4: 1,
                5: timestamp,
                6: region
            }
        }
    }
    
    Pk = (await CrEaTe_ProTo(fields)).hex()
    Pk = "080112" + await EnC_Uid(len(Pk) // 2, Tp='Uid') + Pk
    return await GeneRaTePk(Pk, '1201', K, V)
    
async def xSEndMsgsQq(Msg , id , K , V, region="IND"):
    
    avatar = get_random_avatar()
    
    fields = {
        1: id, 
        2: id, 
        4: Msg, 
        5: 1756580149, 
        7: 2, 
        8: 904990072, 
        9: {
            1: "xBe4!sTo - C4", 
            2: avatar, 
            4: 330, 
            5: 1001000001, 
            8: "xBe4!sTo - C4", 
            10: 1, 
            11: 1, 
            13: {1: 2}, 
            14: {
                1: 1158053040, 
                2: 8, 
                3: b"\x10\x15\x08\x0A\x0B\x15\x0C\x0F\x11\x04\x07\x02\x03\x0D\x0E\x12\x01\x05\x06"
            }
        }, 
        10: "en", 
        13: {2: 2, 3: 1},
        14: {
            1: {
                1: random.choice([1, 4]),
                2: 1,
                3: random.randint(1, 180),
                4: 1,
                5: int(datetime.now().timestamp()),
                6: region
            }
        }
    }
    
    Pk = (await CrEaTe_ProTo(fields)).hex()
    Pk = "080112" + await EnC_Uid(len(Pk) // 2, Tp='Uid') + Pk
    return await GeneRaTePk(Pk, '1201', K, V)
  
async def AuthClan(CLan_Uid, AuTh, K, V):
    fields = {1: 3, 2: {1: int(CLan_Uid), 2: 1, 4: str(AuTh)}}
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '1201' , K , V)
    
async def AutH_GlobAl(K, V):
    fields = {
    1: 3,
    2: {
        2: 5,
        3: "en"
    }
    }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '1215' , K , V)

async def LagSquad(K,V):
    fields = {
    1: 15,
    2: {
        1: 1124759936,
        2: 1
    }
    }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '0515' , K , V)
    
async def room_invite(K, V, uid):
    fields = {
        1: 22,
        2: {
            1: int(uid)
        }
    }
    proto_bytes = await CrEaTe_ProTo(fields)
    packet_hex = proto_bytes.hex()
    return await GeneRaTePk(packet_hex, '0E15', K, V)

async def GeT_Status(PLayer_Uid , K , V):
    PLayer_Uid = await EnC_Uid(PLayer_Uid , Tp = 'Uid')
    if len(PLayer_Uid) == 8: Pk = f'080112080a04{PLayer_Uid}1005'
    elif len(PLayer_Uid) == 10: Pk = f"080112090a05{PLayer_Uid}1005"
    return await GeneRaTePk(Pk , '0f15' , K , V)
           
async def SPam_Room(Uid , Rm , Nm , K , V):
    fields = {1: 78, 2: {1: int(Rm), 2: f"[{ArA_CoLor()}]{Nm}", 3: {2: 1, 3: 1}, 4: 330, 5: 1, 6: 201, 10: xBunnEr(), 11: int(Uid), 12: 1}}
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '0e15' , K , V)    
    
async def ghost_packet(player_id, ghost_name, secret_code, key, iv, region):
    fields = {
        1: 61,
        2: {
            1: int(player_id),
            2: {
                1: int(player_id),
                2: int(time.time()),
                3: f"[B][C][{ArA_CoLor()}]{ghost_name.upper()}",
                5: 12,
                6: 9999999,
                7: 1,
                8: {2: 1, 3: 1},
                9: 3,
            },
            3: str(secret_code),
        }
    }
    proto_hex = (await CrEaTe_ProTo(fields)).hex()
    if region.lower() == "ind":
        pkt_type = "0514"
    elif region.lower() == "bd":
        pkt_type = "0519"
    else:
        pkt_type = "0515"
    return await GeneRaTePk(proto_hex, pkt_type, key, iv)
    
async def GenJoinSquadsPacket(code,  K , V):
    fields = {}
    fields[1] = 4
    fields[2] = {}
    fields[2][4] = bytes.fromhex("01090a0b121920")
    fields[2][5] = str(code)
    fields[2][6] = 6
    fields[2][8] = 1
    fields[2][9] = {}
    fields[2][9][2] = 800
    fields[2][9][6] = 11
    fields[2][9][8] = "1.111.1"
    fields[2][9][9] = 5
    fields[2][9][10] = 1
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '0515' , K , V)   
async def GenJoinGlobaL(owner , code , K, V):
    fields = {
    1: 4,
    2: {
        1: owner,
        6: 1,
        8: 1,
        13: "en",
        15: code,
        16: "OR",
    }
    }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '0515' , K , V)
    
async def tc(key, iv):
    fields = {1: 31, 2: {1: 14572471551}}
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), '0515' , key, iv)
    
async def Team_Public(key, iv):
    fields = {1: 36, 2: {1: 14572471551}}
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), '0515' , key, iv)    
    
async def Team_Private(key, iv):
    fields = {1: 37, 5: {1: 14572471551}}
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), '0515' , key, iv)        

async def FS(key, iv, region="ind"):
    try:
        fields = {
            1: 9,
            2: {
                1: 12480598706,
            }
        }
        
        packet = await CrEaTe_ProTo(fields)
        packet_hex = packet.hex()
        
        encrypted_packet = await encrypt_packet(packet_hex, key, iv)
        
        header_length = len(encrypted_packet) // 2
        header_length_final = dec_to_hex(header_length)
        
        if region.lower() == "ind":
            packet_type = '0514'
        elif region.lower() == "bd":
            packet_type = "0519"
        else:
            packet_type = "0515"
        
        if len(header_length_final) == 2:
            final_packet_hex = packet_type + "000000" + header_length_final + encrypted_packet
        elif len(header_length_final) == 3:
            final_packet_hex = packet_type + "00000" + header_length_final + encrypted_packet
        elif len(header_length_final) == 4:
            final_packet_hex = packet_type + "0000" + header_length_final + encrypted_packet
        elif len(header_length_final) == 5:
            final_packet_hex = packet_type + "000" + header_length_final + encrypted_packet
        elif len(header_length_final) == 6:
            final_packet_hex = packet_type + "00" + header_length_final + encrypted_packet
        else:
            final_packet_hex = packet_type + "000000" + header_length_final + encrypted_packet
        
        print(f"✅ Start match packet created: {len(final_packet_hex)//2} bytes")
        return bytes.fromhex(final_packet_hex)
        
    except Exception as e:
        print(f"❌ Error creating start packet: {e}")
        import traceback
        traceback.print_exc()
        return None

async def GeTSQDaTa(D):
    uid = D['5']['data']['1']['data']
    chat_code = D["5"]["data"]["17"]["data"]
    squad_code = D["5"]["data"]["31"]["data"]
    return uid, chat_code , squad_code

async def AutH_Chat(T , uid, code , K, V):
    fields = {
  1: T,
  2: {
    1: uid,
    3: "en",
    4: str(code)
  }
}
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '1215' , K , V)
async def Msg_Sq(msg, owner, bot, K, V):
    fields = {
    1: 1,
    2: 2,
    2: {
        1: bot,
        2: owner,
        4: msg,
        5: 1757799182,
        7: 2,
        9: {
            1: "Fun1w5a2",
            2: get_random_avatar(),
            3: 909000024,
            4: 330,
            5: 909000024,
            7: 2,
            10: 1,
            11: 1,
            12: 0,
            13: {1: 2},
            14: {
                1: bot,
                2: 8,
                3: "\u0010\u0015\b\n\u000b\u0013\f\u000f\u0011\u0004\u0007\u0002\u0003\r\u000e\u0012\u0001\u0005\u0006"
            }
        },
        10: "ar",
        13: {3: 1},
        14: ""
    }
}
    proto_bytes = await CrEaTe_ProTo(fields)
    return await GeneRaTePk(proto_bytes.hex(), '1215', K, V)
    
async def GeneRaTePk(Pk , N , K , V):
    PkEnc = await EnC_PacKeT(Pk , K , V)
    _ = await DecodE_HeX(int(len(PkEnc) // 2))
    if len(_) == 2: HeadEr = N + "000000"
    elif len(_) == 3: HeadEr = N + "00000"
    elif len(_) == 4: HeadEr = N + "0000"
    elif len(_) == 5: HeadEr = N + "000"
    else: print('ErroR => GeneRatinG ThE PacKeT !! ')
    return bytes.fromhex(HeadEr + _ + PkEnc)
async def OpEnSq(K , V,region):
    fields = {1: 1, 2: {2: "\u0001", 3: 1, 4: 1, 5: "en", 9: 1, 11: 1, 13: 1, 14: {2: 5756, 6: 11, 8: "1.111.5", 9: 2, 10: 4}}}
    if region.lower() == "ind":
        packet = '0514'
    elif region.lower() == "bd":
        packet = "0519"
    else:
        packet = "0515"
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , packet , K , V)

async def cHSq(Nu , Uid , K , V,region):
    fields = {1: 17, 2: {1: int(Uid), 2: 1, 3: int(Nu - 1), 4: 62, 5: "\u001a", 8: 5, 13: 329}}
    if region.lower() == "ind":
        packet = '0514'
    elif region.lower() == "bd":
        packet = "0519"
    else:
        packet = "0515"
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , packet , K , V)

async def SEnd_InV(Nu , Uid , K , V,region):
    
    fields = {1: 2 , 2: {1: int(Uid) , 2: region , 4: int(Nu)}}

    if region.lower() == "ind":
        packet = '0514'
    elif region.lower() == "bd":
        packet = "0519"
    else:
        packet = "0515"
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , packet , K , V)
    
async def ExiT(idT , K , V):
    fields = {
        1: 7,
        2: {
            1: idT,
        }
        }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '0515' , K , V) 
    
    
async def ArohiRefuse(owner,uid, K,V):
    random_banner = f"""ㅤ
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    ㅤㅤㅤㅤㅤ
 """
    fields = {
    1: 5,
    2: {
        1: int(owner),
        2: 1,
        3: int(uid),
        4: random_banner
    }
    }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex() , '0515' , K , V)    
    
async def new_lag(K, I):
    fields = {
        1: 15,
        2: {
            1: 804266360,
            2: 1
        }
    }
    return await GeneRaTePk((await CrEaTe_ProTo(fields)).hex(), '0515', K, I)