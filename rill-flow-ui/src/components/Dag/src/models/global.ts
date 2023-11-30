import { reactive } from 'vue';

export const globalGridAttr = reactive({
  type: 'mesh',
  size: 10,
  color: '#e5e5e5',
  thickness: 1,
  colorSecond: '#d0d0d0',
  thicknessSecond: 1,
  factor: 4,
  bgColor: '#e5e5e5',
  showImage: true,
  repeat: 'watermark',
  angle: 30,
  position: 'center',
  bgSize: JSON.stringify({ width: 150, height: 150 }),
  opacity: 0.1,

  stroke: '#5F95FF',
  strokeWidth: 1,
  connector: 'normal',
  label: '',

  nodeStroke: '#5F95FF',
  nodeStrokeWidth: 1,
  nodeFill: '#ffffff',
  nodeFontSize: 12,
  nodeColor: '#080808',
  nodeUsers: '',
});
