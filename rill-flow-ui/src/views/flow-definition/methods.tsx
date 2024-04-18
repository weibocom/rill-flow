import {BasicColumn} from '/@/components/Table/src/types/table';
import {FormProps} from "antd";
import {getBusinessIdsApi, getFeatureIdsApi} from "@/api/table";
import {useI18n} from '/@/hooks/web/useI18n';

const {t} = useI18n();

export function getInstanceColumns(): BasicColumn[] {
  const formatDate = (timestamp) => {
    const date = new Date(timestamp.value);
    return date.toLocaleString();
  };
  return [
    {
      title: t('routes.flow.definitions.columns.business_id'),
      width: 80,
      dataIndex: 'business_id',
    },
    {
      title: t('routes.flow.definitions.columns.feature_id'),
      width: 80,
      dataIndex: 'feature_id',
    },
    {
      title: t('routes.flow.definitions.columns.alias'),
      width: 80,
      dataIndex: 'alias',
    },
    {
      title: t('routes.flow.definitions.columns.create_time'),
      width: 80,
      dataIndex: 'create_time',
      customRender: formatDate,
    },
    {
      title: t('routes.flow.definitions.columns.update_time'),
      width: 80,
      dataIndex: 'update_time',
      customRender: formatDate,
    },
  ];
}

export function getFormConfig(): Partial<FormProps> {
  return {
    labelWidth: 100,
    schemas: [
      {
        field: 'business',
        component: 'ApiSelect',
        label: t('routes.flow.instances.form_props.business_id'),
        required: false,
        componentProps: ({formModel, formActionType}) => {
          return {
            api: getBusinessIdsApi,
            placeholder: t('routes.flow.instances.form_props.business_placeholder'),
            resultField: 'business_ids',
            labelField: 'name',
            valueField: 'id',
            immediate: true,
            onChange: async (e, v) => {
              const {updateSchema} = formActionType;
              if (e === undefined) {
                updateSchema({
                  field: 'feature',
                  componentProps: {
                    options: [],
                  },
                });
                return;
              }
              let featureResult = await getFeatureIdsApi({"business_id": e})
              const features = featureResult.features
              let featureOptions = []
              for (const key in features) {
                featureOptions.push({"value": features[key], "label": features[key]})
              }
              updateSchema({
                field: 'feature',
                componentProps: {
                  options: featureOptions,
                },
              });

            },
          }
        },
        colProps: {
          xl: 10,
          xxl: 5,
        },
      },
      {
        field: 'feature',
        component: 'ApiSelect',
        label: t('routes.flow.instances.form_props.feature_id'),
        required: false,
        componentProps: {
          options: [],
          placeholder: t('routes.flow.instances.form_props.feature_placeholder'),
        },
        colProps: {
          xl: 10,
          xxl: 5,
        },
      },
    ],
  };
}

export function getFlowApiHost() {
  console.log('getFlowApiHost', window.location, import.meta.env.MODE);
  if (import.meta.env.MODE === 'development') {
    return import.meta.env.VITE_FLOW_API_HOST;
  } else {
    return window.location.origin;
  }
}
