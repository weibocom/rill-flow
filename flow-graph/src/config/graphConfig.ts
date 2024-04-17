import {Shape} from "@antv/x6";

export const graphConfig = {
  grid: {
    size: 10,
    visible: false,
    type: 'doubleMesh',
    args: [
      {
        color: '#cccccc',
        thickness: 1,
      },
      {
        color: '#5F95FF',
        thickness: 1,
        factor: 4,
      },
    ],
  },
  connecting: {
    anchor: 'center',
    connectionPoint: 'anchor',
    allowBlank: false,
    highlight: true,
    snap: true,
    createEdge() {
      return new Shape.Edge({
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
      });
    },
    validateConnection({ sourceView, targetView, sourceMagnet, targetMagnet }) {
      if (sourceView === targetView) {
        return false;
      }
      if (!sourceMagnet) {
        return false;
      }
      return true;
    },
  },
  scroller: {
    enabled: false,
    autoResize: false,
  },
  panning: {
    enabled: true,
    eventTypes: ['leftMouseDown', 'rightMouseDown', 'mouseWheel']
  },
  mousewheel: {
    enabled: true,
    modifiers: ['ctrl', 'meta'],
    minScale: 0.5,
    maxScale: 2,
  },
  highlighting: {
    magnetAvailable: {
      name: 'stroke',
      args: {
        padding: 4,
        attrs: {
          strokeWidth: 4,
          stroke: 'rgba(223,234,255)',
        },
      },
    },
  },
  snapline: true,
  keyboard: {
    enabled: true,
  },
}
