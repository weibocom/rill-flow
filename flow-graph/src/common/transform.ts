import { Lang } from '@antv/x6';

export function getVueNodeConfig(node) {
  const { label, width, height, id, data, position, ports, icon, status } = getBaseConfig(node);
  return {
    id,
    shape: 'vue-shape',
    width,
    height,
    component: 'vue-node',
    label,
    zIndex: 100,
    data,
    position,
    ports,
    attrs: {
      label: { text: label },
    },
  };
}

/**
 * 获取默认配置选项
 * 兼容x6/g6
 */
function getBaseConfig(node) {
  const {
    type,
    shape,
    tooltip,
    attrs,
    x,
    y,
    size,
    id,
    position,
    data,
    actionType,
    icon,
    ports,
    status,
    nodePrototype,
    label,
    name,
  } = node;
  let _width,
    _height,
    _x = x,
    _y = y,
    _shape = shape,
    _tooltip = tooltip,
    _actionType = actionType;
  if (data && data.actionType) {
    _actionType = data.actionType;
  }
  if (size) {
    // G6
    if (Lang.isArray(size)) {
      _width = size[0];
      _height = size[1];
    }
    // x6
    else if (Lang.isObject(size)) {
      _width = size.width;
      _height = size.height;
    }
  }
  if (Lang.isObject(position)) {
    _x = position.x;
    _y = position.y;
  }
  // 形状转义
  // G6
  if (Lang.isString(type)) {
    _shape = type;
    if (type === 'diamond') _shape = 'rect';
  }
  if (Lang.isObject(attrs)) {
    _tooltip = attrs.label.text;
  }

  return {
    type: _shape,
    label: label,
    x: _x,
    y: _y,
    width: _width,
    height: _height,
    id,
    actionType: _actionType,
    data: {
      icon: icon,
      nodePrototype: nodePrototype,
      nodeId: id,
      name: name,
      status: status,
    },
    icon,
    ports,
    status,
    position: position,
  };
}

export function getEdgeConfig() {
  return {
    shape: 'edge',
    attrs: {
      line: {
        stroke: '#5F95FF',
        strokeWidth: 1,
        targetMarker: {
          name: 'classic',
          size: 8,
        },
      },
    },
    router: {
      name: 'manhattan',
    },
    visible: true,
    connector: 'rounded',
    zIndex: 100,
    source: {
      cell: undefined,
      port: undefined,
    },
    target: {
      cell: undefined,
      port: undefined,
    }
  };
}

export const getJsonByJsonPaths = (paths) => {
  const result = {};
  for (var i = 0; i < paths.length; i++) {
    var item = paths[i];
    var keys = item.key.split('.');
    var current = result;

    for (var j = 0; j < keys.length - 1; j++) {
      var key = keys[j];
      if (!current[key]) {
        current[key] = {};
      }
      current = current[key];
    }

    var lastKey = keys[keys.length - 1];
    current[lastKey] = item.value;
  }
  return result;
}

export const getJsonPathByJsonSchema = (data) => {
  // data是一个map，该map的key为string类型，value是map类型。data可以理解为一个树。将其转换成key为非叶子节点以.间隔，value为叶子节点
  const list = [];
  function isBaseType(type) {
    return type === 'string' || type === 'boolean' || type === 'number';
  }
  function addToList(obj, prefix = '', isFinish = false) {
    if (isFinish) {
      if (!list.includes(prefix.slice(0, -1))) {
        list.push(prefix.slice(0, -1));
      }
      return;
    }
    if (typeof obj === 'object') {
      if (isBaseType(obj?.type)) {
        // 去重并加入到list中
        if (!list.includes(prefix.slice(0, -1))) {
          list.push(prefix.slice(0, -1));
        }
        return;
      }

      if (obj?.type === 'array') {
        if (obj?.bizType === 'array-to-map') {
          addToList(obj.items.properties, prefix, true);
        } else {
          // TODO 直接将对应数据保存
          addToList(obj.items.properties, prefix + '*.');
        }
        return;
      }

      if (obj?.properties) {
        for (const key in obj?.properties) {
          addToList(obj.properties[key], prefix + key + '.');
        }
        return;
      }

      for (const key in obj) {
        addToList(obj[key], prefix + key + '.');
      }
    }
  }
  addToList(data);
  return list;
}
