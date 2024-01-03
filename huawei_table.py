from database import Table, Field

# store_sales = Table("Store_Sales", "f") #
# store_sales.addField(Field("ss_sold_date_sk", "identifier", "store_sales"))
# store_sales.addRelationship("Store_Sales", "ss_sold_date_sk", "ss_sold_date_sk")

# q1
ads_rms_orpbase_device_city_period_freq_v2_ds = Table("ads_rms_orpbase_device_city_period_freq_v2_ds", "f")

fields_q1 = [
    Field("imei", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq1_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq2_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq3_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq4_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq5_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq1_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq2_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq3_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq4_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq5_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq1_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq2_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq3_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq4_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_freq5_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_cnt_pt_d", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d3_cnt_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq1_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq2_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq3_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq4_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq5_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq1_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq2_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq3_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq4_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq5_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq1_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq2_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq3_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq4_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_freq5_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_cnt_pt_d", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d7_cnt_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq1_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq2_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq3_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq4_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq5_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq1_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq2_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq3_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq4_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq5_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq1_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq2_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq3_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq4_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_freq5_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_cnt_pt_d", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d15_cnt_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq1_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq2_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq3_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq4_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq5_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq1_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq2_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq3_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq4_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq5_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq1_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq2_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq3_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq4_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_freq5_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_cnt_pt_d", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d30_cnt_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq1_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq2_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq3_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq4_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq5_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq1_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq2_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq3_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq4_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq5_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq1_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq2_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq3_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq4_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_freq5_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_cnt_pt_d", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d90_cnt_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq1_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq2_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq3_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq4_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq5_loc", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq1_cnt", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq2_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq3_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq4_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq5_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq1_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq2_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq3_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq4_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_freq5_loc_rn", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_cnt_pt_d", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),
    Field("d180_cnt_loc", "integer", "ads_rms_orpbase_device_city_period_freq_v2_ds"),

    Field("pt_d", "string", "ads_rms_orpbase_device_city_period_freq_v2_ds")
]
for f in fields_q1:
    ads_rms_orpbase_device_city_period_freq_v2_ds.addFields(f)

# q3

dwd_push_pre_devapp_token_sf_ds = Table("dwd_push_pre_devapp_token_sf_ds", "f")

fields_q3 = [
    Field("did", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("package_name", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("senderid", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("imei", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("push_token", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("goods_arrive_country_cd", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("ori_imei", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("app_id", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("token_id", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("app_status", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("first_apply_date", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("last_apply_date", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("last_apply_time", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("first_uninstall_date", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("last_uninstall_date", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("last_uninstall_time", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("year5_active_flg", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("did_last_active_date", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("user_id", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("etl_time", "string", "dwd_push_pre_devapp_token_sf_ds"),
    Field("device_id_type_cd", "string", "dwd_push_pre_devapp_token_sf_ds"),

    Field("pt_d", "string", "dwd_push_pre_devapp_token_sf_ds")

]
for f in fields_q3:
    dwd_push_pre_devapp_token_sf_ds.addFields(f)

# q5
dwd_hms_appaccesssdk_active_tf_dm_account = Table("dwd_hms_appaccesssdk_active_tf_dm_account", "f")

fields_q5 = [
    Field("etl_surid", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("up_id", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("did", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("hw_did", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_inside_modname", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_extnal_modname", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_prod_sprdname", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_brand", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_categ", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_series_name", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_emui_ver", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_price_scope", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_launch_date", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("device_rlstype", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("huawei_pay_flag", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("report_ip", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("oper_id", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("report_time", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("oper_time", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("app_surid", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("app_id", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("app_cnname", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("app_first_class_name", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("app_second_class_name", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("app_project_name", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("app_ver", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("sdk_ver_surid", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("sdk_ver", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("sdk_ver_type", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("package_name", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("scenes", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("data_src", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("interface_name", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("main_sdk_ver", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("success_flg", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("audit_log", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("etl_time", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("allian_onshelve_flg", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("return_code_cd", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("return_code", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("success_cnt", "integer", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("fail_cnt", "integer", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("req_cnt", "integer", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("hispace_onshelve_flg", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("kit_ver", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("country_cd", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("sub_service", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("trans_id", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("cost_time", "integer", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("network_type", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("apk_ver", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("scene_type", "string", "dwd_hms_appaccesssdk_active_tf_dm_account"),
    Field("pt_d", "string", "dwd_hms_appaccesssdk_active_tf_dm_account")
]
for f in fields_q5:
    dwd_hms_appaccesssdk_active_tf_dm_account.addFields(f)

# q6
ads_hiboard_video_cpid_ecp_v1_dm = Table("ads_hiboard_video_cpid_ecp_v1_dm", "f")

fields_q6 = [
    Field("emui_ver", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("app_ver", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("cpid", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("prod_name", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("exposure_cnts", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("exposure_users", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("exposure_count", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("exposure_count_users", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("click_cnts", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("click_users", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("click_count", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("click_count_users", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("play_cnts", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("play_users", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("play_count", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("play_count_users", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("playtime", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("duration", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("playtime_s", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),
    Field("duration_s", "string", "ads_hiboard_video_cpid_ecp_v1_dm"),

    Field("pt_d", "string", "ads_hiboard_video_cpid_ecp_v1_dm")
]
for f in fields_q6:
    ads_hiboard_video_cpid_ecp_v1_dm.addFields(f)

# q8
ads_adv_basic_operate_prod_name_dm = Table("ads_adv_basic_operate_prod_name_dm", "f")

fields_q8 = [
    Field("statis_cycle_sign", "string", "ads_adv_basic_operate_prod_name_dm"),
    Field("index_flg", "string", "ads_adv_basic_operate_prod_name_dm"),
    Field("adv_type", "string", "ads_adv_basic_operate_prod_name_dm"),
    Field("brand", "string", "ads_adv_basic_operate_prod_name_dm"),
    Field("app_name", "string", "ads_adv_basic_operate_prod_name_dm"),
    Field("task_name", "string", "ads_adv_basic_operate_prod_name_dm"),
    Field("statis_dim", "string", "ads_adv_basic_operate_prod_name_dm"),
    Field("device_cnt", "integer", "ads_adv_basic_operate_prod_name_dm"),
    Field("pt_d", "string", "ads_adv_basic_operate_prod_name_dm")
]
for f in fields_q8:
    ads_adv_basic_operate_prod_name_dm.addFields(f)


# q10
ads_hwread_app_install_type_filter_sum_dm = Table("ads_hwread_app_install_type_filter_sum_dm", "f")

fields_q10 = [
    Field("app_ver", "string", "ads_hwread_app_install_type_filter_sum_dm"),
    Field("inside_name", "string", "ads_hwread_app_install_type_filter_sum_dm"),
    Field("emui_ver", "string", "ads_hwread_app_install_type_filter_sum_dm"),
    Field("device_num", "integer", "ads_hwread_app_install_type_filter_sum_dm"),
    Field("app_preinstall_users", "integer", "ads_hwread_app_install_type_filter_sum_dm"),
    Field("hispace_install_users", "integer", "ads_hwread_app_install_type_filter_sum_dm"),

    Field("pt_d", "string", "ads_hwread_app_install_type_filter_sum_dm"),
    Field("pt_dim", "string", "ads_hwread_app_install_type_filter_sum_dm")

]
for f in fields_q10:
    ads_hwread_app_install_type_filter_sum_dm.addFields(f)


# q11
ads_opensdk_app_basic_statistics_info_dm_res = Table("ads_opensdk_app_basic_statistics_info_dm_res", "f")

fields_q11 = [
    Field("imei", "string", "ads_opensdk_app_basic_statistics_info_dm_res"),
    Field("first_active_time", "string", "ads_opensdk_app_basic_statistics_info_dm_res"),
    Field("final_active_time", "string", "ads_opensdk_app_basic_statistics_info_dm_res"),
    Field("dev_app_id", "string", "ads_opensdk_app_basic_statistics_info_dm_res"),

    Field("pt_d", "string", "ads_opensdk_app_basic_statistics_info_dm_res"),
]
for f in fields_q11:
    ads_opensdk_app_basic_statistics_info_dm_res.addFields(f)


# q23

dwd_push_pre_msg_mc_tf_hm = Table("dwd_push_pre_msg_mc_tf_hm", "f")

fields_q23 = [
    Field("etl_surid", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("imei", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("country", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("ori_imei", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("ori_device_id_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("up_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("device_appver", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("report_ip", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("report_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("oper_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("resp_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("anl_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("log_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("log_type_cd", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("send_result", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("send_result_cd", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("app_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("push_app_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("device_token", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("cache_flg", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("former_msg_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_ver", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("send_date", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_type_cd", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("task_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("task_id_old", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("send_channel", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("send_channel_cd", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("expire_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("expire_date", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("user_trace_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("anl_task_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_req_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("session_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("mass_flg", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_arrive_flg", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_discard_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_arrive_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_non_arrive_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("audit_log", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("etl_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("device_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("cache_rsd_flg", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("cache_rsd_flg_cd", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("business_type_cd", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("business_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("apigw_recieve_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("slb_recieve_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("project_id", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("send_channel_sub_type_cd", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("send_channel_sub_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("in_queue_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("apigw_call_scheduler_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("out_queue_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("flow_scheduler_resour", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("flow_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("class_type", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("client_rcv_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("send_push_server_time", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("msg_level", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("cam_tags", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("belong_id", "string", "dwd_push_pre_msg_mc_tf_hm"),

    Field("pt_d", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("pt_h", "string", "dwd_push_pre_msg_mc_tf_hm"),
    Field("pt_msg", "string", "dwd_push_pre_msg_mc_tf_hm")
]

for f in fields_q23:
    dwd_push_pre_msg_mc_tf_hm.addFields(f)


# q25

dwd_up_oper_tf_dm = Table("dwd_up_oper_tf_dm", "f")

fields_q25 = [
    Field("etl_surid", "string", "dwd_up_oper_tf_dm"),
    Field("up_id", "string", "dwd_up_oper_tf_dm"),
    Field("device_module_id", "string", "dwd_up_oper_tf_dm"),
    Field("device_module_idtype", "string", "dwd_up_oper_tf_dm"),
    Field("did", "string", "dwd_up_oper_tf_dm"),
    Field("hw_did", "string", "dwd_up_oper_tf_dm"),
    Field("device_brand", "string", "dwd_up_oper_tf_dm"),
    Field("device_categ", "string", "dwd_up_oper_tf_dm"),
    Field("device_series_name", "string", "dwd_up_oper_tf_dm"),
    Field("device_prod_sprdname", "string", "dwd_up_oper_tf_dm"),
    Field("device_inside_modname", "string", "dwd_up_oper_tf_dm"),
    Field("device_extnal_modname", "string", "dwd_up_oper_tf_dm"),
    Field("device_emui_ver", "string", "dwd_up_oper_tf_dm"),
    Field("device_price_scope", "string", "dwd_up_oper_tf_dm"),
    Field("device_launch_date", "string", "dwd_up_oper_tf_dm"),
    Field("up_oper_client_ip", "string", "dwd_up_oper_tf_dm"),
    Field("ip_country", "string", "dwd_up_oper_tf_dm"),
    Field("ip_province_encode", "string", "dwd_up_oper_tf_dm"),
    Field("ip_province", "string", "dwd_up_oper_tf_dm"),
    Field("ip_city_encode", "string", "dwd_up_oper_tf_dm"),
    Field("ip_city", "string", "dwd_up_oper_tf_dm"),
    Field("req_time", "string", "dwd_up_oper_tf_dm"),
    Field("oper_time", "string", "dwd_up_oper_tf_dm"),
    Field("server_name", "string", "dwd_up_oper_tf_dm"),
    Field("interface_name", "string", "dwd_up_oper_tf_dm"),
    Field("oper_type_name", "string", "dwd_up_oper_tf_dm"),
    Field("up_interface_active_flg", "string", "dwd_up_oper_tf_dm"),
    Field("up_oper_req_channel_id", "string", "dwd_up_oper_tf_dm"),
    Field("up_oper_req_channel_name", "string", "dwd_up_oper_tf_dm"),
    Field("up_oper_req_channel_type", "string", "dwd_up_oper_tf_dm"),
    Field("up_oper_req_app_id", "string", "dwd_up_oper_tf_dm"),
    Field("up_oper_req_app_package_name", "string", "dwd_up_oper_tf_dm"),
    Field("channel_blng_business_cn_name_org", "string", "dwd_up_oper_tf_dm"),
    Field("etl_time", "string", "dwd_up_oper_tf_dm"),
    Field("orig_imei", "string", "dwd_up_oper_tf_dm"),
    Field("device_name", "string", "dwd_up_oper_tf_dm"),
    Field("device_type", "string", "dwd_up_oper_tf_dm"),

    Field("pt_d", "string", "dwd_up_oper_tf_dm"),
    Field("pt_service", "string", "dwd_up_oper_tf_dm")
    ]
for f in fields_q25:
    dwd_up_oper_tf_dm.addFields(f)

# q26
dwd_up_urgent_active_tf_dm = Table("dwd_up_urgent_active_tf_dm", "f")

fields_q26 = [
    Field("etl_surid", "string", "dwd_up_urgent_active_tf_dm"),
    Field("up_id", "string", "dwd_up_urgent_active_tf_dm"),
    Field("channel_src", "string", "dwd_up_urgent_active_tf_dm"),
    Field("channel_src_cd", "string", "dwd_up_urgent_active_tf_dm"),
    Field("oper_id", "string", "dwd_up_urgent_active_tf_dm"),
    Field("oper_name", "string", "dwd_up_urgent_active_tf_dm"),
    Field("error_code", "string", "dwd_up_urgent_active_tf_dm"),
    Field("error_desc", "string", "dwd_up_urgent_active_tf_dm"),
    Field("app_ver", "string", "dwd_up_urgent_active_tf_dm"),
    Field("audit_log", "string", "dwd_up_urgent_active_tf_dm"),
    Field("etl_time", "string", "dwd_up_urgent_active_tf_dm"),

    Field("pt_d", "string", "dwd_up_urgent_active_tf_dm")
]
for f in fields_q26:
    dwd_up_urgent_active_tf_dm.addFields(f)


