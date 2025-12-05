schema: own_91109_svg_um

tables:

  identification:
    name: t_tpn_identification
    columns:
      - tpn_id
      - comp_name
      - incrp_count
      - d_incorp
      - n_emp
      - leg_form
      - d_lei_cert
      - d_immat_lei
      - s_lei
      - vat_num
      - sta_vat_num
      - loc_act_code_type
      - loc_act_code
      - nace_code
      - tp_rel_own
      - business_cntry
      - legal_status
      - reg_req
      - data_share_stat
      - data_source
      - cert_status
      - cert_owner
      - val_dat
      - csr_flag
      - c_typo

  address:
    name: t_tpn_address
    columns:
      - tpn_id
      - address_type
      - address_distribution
      - address_way
      - address_hamlet
      - address_postcode_city
      - address_country
      - dept_iso
      - subdept_iso
      - building_name_iso
      - floor_iso
      - room_iso
      - street_name_iso
      - building_number_iso
      - post_box_iso
      - town_location_name_iso
      - post_code_iso
      - town_name_iso
      - country_iso
      - district_name_iso
      - country_subdivision_iso

  company_identifier:
    name: t_tpn_company_identifier
    derived_mapping:
      - source_col: national_registry_id
        comp_id_nat: national_registry
        comp_id_name: National registry
      - source_col: international_registry_id
        comp_id_nat: international_registry
        comp_id_name: International registry
      - source_col: market_data_id
        comp_id_nat: market_data
        comp_id_name: Market data
      - source_col: bnpp_business_is_id
        comp_id_nat: bnpp_business_is
        comp_id_name: BNPP Business ID
