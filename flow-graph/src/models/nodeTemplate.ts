import {getNodeCategoryByNumber, NodeCategory} from "./enums/nodeCategory";

export class NodePrototypeRegistry {
  private nodePrototypeMap: Map<string, NodePrototype> = new Map<string, NodePrototype>();

  public getNodePrototype(id: string): NodePrototype {
    return this.nodePrototypeMap.get(id);
  }

  public getNodePrototypes(): NodePrototype[] {
    return Array.from(this.nodePrototypeMap.values());
  }

  public add(nodePrototype: NodePrototype) {
    this.nodePrototypeMap.set(nodePrototype.id, nodePrototype);
  }

  public addAll(nodePrototypes: NodePrototype[]) {
    nodePrototypes.forEach(nodePrototype => {
      this.add(nodePrototype);
    });
  }

  public remove(id: string) {
    this.nodePrototypeMap.delete(id);
  }

  public clear() {
    this.nodePrototypeMap.clear();
  }

  public isEmpty(): boolean {
    return this.nodePrototypeMap.size === 0;
  }
}

export class NodePrototype {
  id: string;
  icon: string;
  node_category: number; // 节点分类
  meta_data: Meta;
  template: Template;
}

export class Meta {
  category: string;
  icon: string;
  fields: object;
}

export class Template {
  name: string;
  type: number;
  type_str: string;
  node_type: string;
  category: string;
  icon: string;
  task_yaml: string;
  schema: string;
  output: string;
  enable: number;
  create_time: Date;
  update_time: Date;
}
