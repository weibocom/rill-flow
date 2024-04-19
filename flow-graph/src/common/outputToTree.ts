export function convertSchemaToTreeData(schema, currentPath = '$') {
  const treeData = [];

  if (schema.type === 'object') {
    if (schema.properties === undefined) {
      return treeData;
    }
    for (const [key, value] of Object.entries(schema.properties)) {
      currentPath += '.' + key;
      const node = {
        title: key + '【' + value.type + '】',
        value: currentPath,
        children: convertSchemaToTreeData(value, currentPath),
      };
      treeData.push(node);
    }
  } else if (schema.type === 'array') {
    const arrayItems = schema.items;

    if (arrayItems && arrayItems.type === 'object') {
      currentPath += '.*';
      return convertSchemaToTreeData(arrayItems, currentPath);
    }
  }

  return treeData;
}

const schema = {
  type: 'object',
  properties: {
    statuses: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          visible: {
            type: 'object',
            properties: {
              type: { type: 'string' },
              list_id: { type: 'string' },
            },
          },
          created_at: { type: 'string' },
          id: { type: 'integer' },
          mid: { type: 'string' },
        },
      },
    },
  },
};

const treeData = convertSchemaToTreeData(schema);
