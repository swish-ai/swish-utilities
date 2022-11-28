#!/bin/bash

ziped_image="H4sIALOFa2MAA+2bP08bQRDF+3wKujQpHCUSEnwE60wJUqgSOiC0LvzhI0W6O/u3fvt2ds8Y5O14BnyzszNv/t7ucfv9968f16/D/x9Wrx133PFyeBjux482B3/y83UNvAJ+Bt4CvwG/AP8BvgG+Bf6Wle90Kto9/t2d+5o67vgS8D18nBxBDiAmhxDz+8hJxOQw4q8Gv2TP1zmr41OG9mH86AFmyVB+Z/AKj9hMjzj3IetwqgHnyM/TiYdJr2kKxP86V8rS6eDy8GyVTNwZtFxQfAJmYs/fu0T9CRK/VxCs1yDTAPr1jTjREH7k+AGLL+qUErwJCRKJ9qJAZ4eOmWC7hJZ2Nkf+2VuisXS99x3DccFyBj1+wqSGtMPH3iXfEPTSlAndSV25QkyvJ1Pztu4g8KSk7tKXgtNwXFqDJm6gnNGzBO3WtQopMuOtK+MdpjzEzHiIeb7D3zfEe4UZ/ikQL4AKpcKZAhKzwHMpnUuARi7uvPP58RzYXbijldCqGKFdG4JWSLduDbekGXoRy/MN8KShWremghzvPJvvXxpnqoiwFSkduEsdddKp5D1cXV6Tixjkhuh0j5i+T9/k852vuoKa3LAV/x+3/GLPGLJHSoP+rTnyR7Gq5a1UaUQFpAruuLo6+zkX8GZprMzoaWt0N9czzGfIKT3kU4NUPkU3te6Ynlj15tMnDErI/IZE6qDukNF850mKbE1FicSUTE5+mo1VSeCUMAaaz5gfDJmAS827dRo3ynYlo+txEz9MZyj1OJVuN9uO8h92Opptk3cUTXpcGR3FawpoT8A/iGbjqh9cTTn1Q9NzOLxsUMcjqFtlKZ1AU569RrwLcWpsVet/fJ7suC/l8PkGwbEhQcVFtxbti7HQoI5dTRuJWuZP2Jtx1MWh7viNS3hp4DJcnkqGJoOzjHQpmiMrHSV1FVNqMMKovX2stFDqK2MT8MwkqmQALTnWPaXawJTVR0mZ49FmgdruARNAfd0u7adFucSmtLuSISCKlJ8JlWw46JxaKya6h1GckZXheEIWHY1RreTx4gJPhafFp2Px+CdJudItfRjQjHvgh6QbN3ZjpIg1GOJzXDZBDT2lN8UDlvJjoiEdNEq7ntPduXqjvNCVLOHW0FV0LDtjmmir7pl3WvWNur+mU8RosNTtqvY01J58zktTi3B1Ei3CF3/RFUnliBlNRbv/7tWKapKsfWCetTPTV30N3NNIt9UynKZTEdenLVZdfUm31OxHntElmkqAkOM5b97bslcHdSHPWSFNRjYpF/MDR5sv86G1ZUftw61TUo1uVSO6CpLfVIyOb9OISPncQpNLq/l7F3GJ3ainNXZqs5xtJpqkli/E7vswLcEt3eQ7Ln5JKDpks0ynkrMD/4vuNLt5p3uHIjpvdSVP6wale0ck+iJl9MVK9/J4FDv5WzdKo5ve0RauWwh17+y4dRoXmZdqahMflMduJBpNmljOkjqCZVQqkLtlCuz2nqNW7Lyq1Srya8XRKNgyn7l0PCmsNbFzm+Hu1eBoIHMmLF8watXYsUHUHN9dJ4z8n9+Liastppb0/51nKk/sntdxx6fEOd65TjrBzKtYx7MpRsy/ZweMKRAj9kfRWscdd3xSVvryDzojrONNUQAA"




if [[ "$@" == *--logo* ]]; then
    if  command -v printf &> /dev/null
    then
        if  command -v base64 &> /dev/null
        then
            image_str=`echo $ziped_image | base64 -d | gunzip`
            printf "$image_str"
            echo
            echo
            echo
        fi
    fi
fi


if [ ! -f swish-utilities ]
then
    echo "swish-utility executable should be located in the same directory with" `basename "$0"`
    exit 1
fi

if [[ ! -x swish-utilities ]]
then
    echo 'swish-utilities is not executable. Perhaps you need to execute command "chmod +x swish-utilities"'
    exit 1
fi

touch extraction_config

# load properties from config file
config=`cat extraction_config`
IFS=$'\n' read -rd '' -a y <<<"$config"
for line in "${y[@]}"
do
    keyVal=(${line//=/ })
    declare "${keyVal[0]}=${keyVal[1]}"
done

echo "================ Welcome to the Swish helper ======="
echo

while [ ! $snow_base_url ]; do
    echo "Enter ServiceNow base url (i.e https://dev87557.service-now.com):"
    read snow_base_url
    if [ $snow_base_url ]; then
        echo "snow_base_url=$snow_base_url" >> extraction_config
    fi
done

read_periud () {
    y=`date +%Y`
    py=$(($y-1))
    a="This year ($y)"
    b="Previous year ($py)"
    PS3="Select year for extraction start: "
    select opt in "$a" "$b"; do
    case $opt in
            $a)
                sy=$y
                break
            ;;
            $b)
                sy=$py
                break
            ;;
            *) 
                echo "Invalid option $REPLY"
            ;;
        esac
    done

    months=(January February March April May June July August September October November December)

    echo
    PS3="Select start month: "
    select opt in "${months[@]}"; do
        if [ "$REPLY" -ge 1 ] && [ "$REPLY" -le 9 ]; then
            sm="0$REPLY"
            break
        elif  [ "$REPLY" -le 12 ]; then
            sm="$REPLY"
            break
        else
            echo "Invalid option $REPLY"
        fi
    done
    echo
    echo "Select number of months to extarct (1-12) default 3. Select -1 for days selection"
    month_range=""

    while :
    do
        read month_range
        if [ ! $month_range ]; then
            month_range=3
            break
        fi
        if [ $month_range -eq -1 ] ;then
            selected_days=1
            echo "Select number of days to extarct default 1"
            ###### Read days count
            while :
                do
                    read days_range
                    if [ ! $days_range ]; then
                        days_range=1
                        break
                    fi
                    if [ $days_range -le 0 ]; then
                        echo "Invalid selection. Please select positive number"
                    else
                        break
                    fi
                done
            break

        ### end read days count
        elif [ $month_range -le 0 ] || [ $month_range -ge 13 ]; then
            echo "Invalid selection. Please select a number between 1 and 12"
        else
            break
        fi
    done
    

    start_date="$sy-$sm-01"
    delta_str="+$month_range months"
    mac_delta_str="-v+"$month_range"m"
    
    if [ $days_range ]; then
        delta_str="+$days_range days"
        mac_delta_str="-v+""$days_range""d"
    fi
 
    end_date=`date +%Y-%m-%d -d "$start_date $delta_str"`
    if [ $? -ne 0 ]; then
	echo $end_date
        end_date=`date -j -f %Y-%m-%d $mac_delta_str $start_date +%Y-%m-%d`
    fi
}


while [ ! $config_json ]; do
    echo "Enter path to the config.json (i.e config_4_8.json):"
    read config_json
    if [ ! -f $config_json ]; then
        echo "Can't find $config_json. Please provide full path or copy it into this script folder"
    else
        echo "config_json=$config_json" >> extraction_config
    fi
done

read_periud


echo "Please enter ServiceNow account username"
read username
echo "Please enter ServiceNow account password"
read -s password


if [[ $snow_base_url != https* ]]; then
    snow_base_url = "https://$snow_base_url"
fi


echo "============   Authenticating  ==============="
echo
rm -rf otf
./swish-utilities --token_get --snow_url "$snow_base_url" --username "$username" --password "$password" --out_token_file otf
token=`cat otf 2>/dev/null`
rm -rf otf

while [ $token == 'error' ] || [ ! $token ]; do
    echo -e "\033[0;31mFailed to authenticate. Trying again.\033[m"
    echo "Please enter ServiceNow account username"
    read username
    echo "Please enter ServiceNow account password"
    read -s password
    ./swish-utilities --token_get --snow_url "$snow_base_url" --username "$username" --password "$password" --out_token_file otf
    token=`cat otf 2>/dev/null`
    rm -rf otf
done


echo "============   Starting data extraction  ==============="
echo

./swish-utilities --extract \
--url "$snow_base_url"/api/now/table/sys_audit?sysparm_query=tablename=incident \
--token "$token" \
--start_date "$start_date" \
--end_date "$end_date" \
--config "$config_json"

echo "Finished scrip execution with status code $?"





