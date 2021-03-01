SELECT crg.research_group_name, crg.research_group_contact_name, crg.research_group_contact_phone, crg.sourcing, st.st, st.is_default AS is_st_default FROM casmart_research_group AS crg INNER JOIN ship_to_casmart_research_group AS stcrg ON stcrg.research_group_id = crg.research_group_id INNER JOIN ship_to AS st ON stcrg.st = st.st WHERE crg.updated_date >= NOW() - INTERVAL '{0} HOURS' OR st.updated_date >= NOW() - INTERVAL '{0} HOURS'