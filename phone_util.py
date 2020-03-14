from __future__ import print_function
import re


def normalize_phone(phone_number):
    """
    input: phone like 1676363888, return String: like 0376363888
    """
#     if not phone_number or len(phone_number) == 0:
#         return ""
    a = [('0162', '032'), ('0163', '033'), ('0164', '034'), ('0165', '035'), ('0166', '036'), ('0167', '037'), ('0168', '038'), ('0169', '039'), ('0120', '070'), ('0121', '079'), ('0122', '077'), ('0126', '076'), ('0128', '078'), ('0123', '083'), ('0124', '084'), ('0125', '085'), ('0127', '081'), ('0129', '082'), ('0186', '056'), ('0188', '058'), ('0199', '059')]
    mapping = {x[0]:x[1] for x in a}
    phone_number = str(phone_number)
    phone_number = re.sub("^\+","", phone_number)
    phone_number = re.sub("^84","",phone_number)
    phone_number = re.sub("^0","",phone_number)

    if phone_number is None:
        return ""
    phone_number = "0" + phone_number
    if len(phone_number) == 10 or len(phone_number) < 5:
        return phone_number
    else:
        first_four_digits = phone_number[0:4]
        if first_four_digits in mapping:
            new_first_three_digits = mapping[first_four_digits]
            phone_number = new_first_three_digits + phone_number[4:]
            return phone_number
        else:
            return phone_number


def get_agent(phone):
    phone = str(phone)
    viettel = ['8486', '8497', '8498', '8432', '8434', '8435', '8496', '8433', '8439', '8438', '8436', '8437']
    viettel_list = [[a,"viettel"] for a in viettel]
    mobi = ['8479', '8493', '8477', '8490', '8478', '8476', '8489', '8470']
    mobi_list = [[a,"mobi"] for a in mobi]
    vina = ['8481', '8494', '8491', '8488', '8483', '8484', '8482', '8485']
    vina_list = [[a,"vina"] for a in vina]
    all_list = viettel_list + mobi_list + vina_list
    prefix_dict = {key:value for (key, value) in all_list}
    prefix = phone[:4]
    return prefix_dict.get(prefix, "other")


