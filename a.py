import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import os

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

def draw_horizontal_flowchart():
    # 创建横向画布 (宽>高)
    fig, ax = plt.subplots(1, 1, figsize=(16, 8))
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 8)
    ax.axis('off')
    
    # 定义颜色
    color_input = '#E3F2FD'      # 浅蓝 - 输入数据
    color_process = '#FFF3E0'    # 浅橙 - 处理过程
    color_branch = '#FCE4EC'     # 浅粉 - 分支
    color_model = '#E8F5E9'      # 浅绿 - 模型训练
    color_eval = '#F3E5F5'       # 浅紫 - 评估
    color_result = '#FFF9C4'     # 浅黄 - 结果
    
    # ==================== 1. 输入数据 ====================
    box1 = FancyBboxPatch((0.5, 3.2), 1.8, 1.6, 
                           boxstyle="round,pad=0.05",
                           facecolor=color_input, edgecolor='#333', linewidth=1.5)
    ax.add_patch(box1)
    ax.text(0.5+0.9, 3.2+0.8, 'ExDark\n原始数据集', fontsize=10, ha='center', va='center', fontweight='bold')
    ax.text(0.5+0.9, 3.2+0.3, '7,363张图像\n12类目标', fontsize=7, ha='center', va='center', color='#555')
    
    # 箭头 1
    ax.annotate('', xy=(2.5, 4.0), xytext=(2.3, 4.0),
                arrowprops=dict(arrowstyle='->', color='#666', lw=1.5))
    
    # ==================== 2. 数据预处理 ====================
    box2 = FancyBboxPatch((2.5, 3.0), 2.0, 1.8, 
                           boxstyle="round,pad=0.05",
                           facecolor=color_process, edgecolor='#333', linewidth=1.5)
    ax.add_patch(box2)
    ax.text(2.5+1.0, 3.0+1.0, '数据预处理', fontsize=10, ha='center', va='center', fontweight='bold')
    ax.text(2.5+1.0, 3.0+0.6, '图像格式统一', fontsize=7, ha='center', va='center')
    ax.text(2.5+1.0, 3.0+0.3, '标注格式转换', fontsize=7, ha='center', va='center')
    ax.text(2.5+1.0, 3.0+0.0, '8:1:1划分', fontsize=7, ha='center', va='center')
    
    # 箭头 2 (分叉)
    ax.annotate('', xy=(4.7, 4.0), xytext=(4.5, 4.0),
                arrowprops=dict(arrowstyle='->', color='#666', lw=1.5))
    
    # ==================== 3. 三个分支 ====================
    branch_y = 3.0
    branch_width = 1.6
    branch_height = 1.8
    
    # 分支1 - Baseline
    box_b1 = FancyBboxPatch((5.0, branch_y), branch_width, branch_height,
                             boxstyle="round,pad=0.05", facecolor=color_branch, edgecolor='#333', linewidth=1.5)
    ax.add_patch(box_b1)
    ax.text(5.0+branch_width/2, branch_y+branch_height/2+0.2, 'Baseline', 
            fontsize=9, ha='center', va='center', fontweight='bold')
    ax.text(5.0+branch_width/2, branch_y+branch_height/2-0.2, '原始数据', 
            fontsize=7, ha='center', va='center', color='#555')
    
    # 分支2 - LLDB
    box_b2 = FancyBboxPatch((7.0, branch_y), branch_width, branch_height,
                             boxstyle="round,pad=0.05", facecolor=color_branch, edgecolor='#333', linewidth=1.5)
    ax.add_patch(box_b2)
    ax.text(7.0+branch_width/2, branch_y+branch_height/2+0.2, 'LLDB', 
            fontsize=9, ha='center', va='center', fontweight='bold')
    ax.text(7.0+branch_width/2, branch_y+branch_height/2-0.2, 'LLDB增强数据', 
            fontsize=7, ha='center', va='center', color='#555')
    
    # 分支3 - Mixed
    box_b3 = FancyBboxPatch((9.0, branch_y), branch_width, branch_height,
                             boxstyle="round,pad=0.05", facecolor=color_branch, edgecolor='#333', linewidth=1.5)
    ax.add_patch(box_b3)
    ax.text(9.0+branch_width/2, branch_y+branch_height/2+0.2, 'Mixed', 
            fontsize=9, ha='center', va='center', fontweight='bold')
    ax.text(9.0+branch_width/2, branch_y+branch_height/2-0.2, '原始+增强 1:1', 
            fontsize=7, ha='center', va='center', color='#555')
    
    # 从预处理到分支的箭头
    ax.annotate('', xy=(5.0+branch_width/2, branch_y+branch_height), 
                xytext=(4.7, 4.0), arrowprops=dict(arrowstyle='->', color='#666', lw=1.2))
    ax.annotate('', xy=(7.0+branch_width/2, branch_y+branch_height), 
                xytext=(4.7, 4.0), arrowprops=dict(arrowstyle='->', color='#666', lw=1.2))
    ax.annotate('', xy=(9.0+branch_width/2, branch_y+branch_height), 
                xytext=(4.7, 4.0), arrowprops=dict(arrowstyle='->', color='#666', lw=1.2))
    
    # 从三个分支汇聚的箭头
    ax.annotate('', xy=(11.0, 4.0), xytext=(5.0+branch_width/2, branch_y), 
                arrowprops=dict(arrowstyle='->', color='#666', lw=1.2))
    ax.annotate('', xy=(11.0, 4.0), xytext=(7.0+branch_width/2, branch_y), 
                arrowprops=dict(arrowstyle='->', color='#666', lw=1.2))
    ax.annotate('', xy=(11.0, 4.0), xytext=(9.0+branch_width/2, branch_y), 
                arrowprops=dict(arrowstyle='->', color='#666', lw=1.2))
    
    # ==================== 4. YOLOv8训练 ====================
    box4 = FancyBboxPatch((11.0, 3.0), 1.8, 1.8, 
                           boxstyle="round,pad=0.05",
                           facecolor=color_model, edgecolor='#333', linewidth=1.5)
    ax.add_patch(box4)
    ax.text(11.0+0.9, 3.0+1.0, 'YOLOv8s', fontsize=10, ha='center', va='center', fontweight='bold')
    ax.text(11.0+0.9, 3.0+0.6, '640×640', fontsize=7, ha='center', va='center')
    ax.text(11.0+0.9, 3.0+0.3, 'batch=8, epochs=50', fontsize=7, ha='center', va='center')
    ax.text(11.0+0.9, 3.0+0.0, 'lr=0.005', fontsize=7, ha='center', va='center')
    
    # 箭头
    ax.annotate('', xy=(13.0, 4.0), xytext=(12.8, 4.0),
                arrowprops=dict(arrowstyle='->', color='#666', lw=1.5))
    
    # ==================== 5. 模型评估 ====================
    box5 = FancyBboxPatch((13.0, 3.0), 1.6, 1.8, 
                           boxstyle="round,pad=0.05",
                           facecolor=color_eval, edgecolor='#333', linewidth=1.5)
    ax.add_patch(box5)
    ax.text(13.0+0.8, 3.0+1.0, '模型评估', fontsize=10, ha='center', va='center', fontweight='bold')
    ax.text(13.0+0.8, 3.0+0.6, 'mAP@0.5', fontsize=7, ha='center', va='center')
    ax.text(13.0+0.8, 3.0+0.3, 'mAP@0.5:0.95', fontsize=7, ha='center', va='center')
    
    # 箭头
    ax.annotate('', xy=(14.8, 4.0), xytext=(14.6, 4.0),
                arrowprops=dict(arrowstyle='->', color='#666', lw=1.5))
    
    # ==================== 6. 对比分析 ====================
    box6 = FancyBboxPatch((14.8, 2.8), 1.4, 1.6, 
                           boxstyle="round,pad=0.05",
                           facecolor=color_result, edgecolor='#333', linewidth=1.5)
    ax.add_patch(box6)
    ax.text(14.8+0.7, 2.8+0.8, '对比分析', fontsize=9, ha='center', va='center', fontweight='bold')
    ax.text(14.8+0.7, 2.8+0.4, 'Baseline', fontsize=7, ha='center', va='center')
    ax.text(14.8+0.7, 2.8+0.1, 'vs LLDB vs Mixed', fontsize=6, ha='center', va='center', color='#D32F2F')
    
    # ==================== 添加标题 ====================
    ax.text(7.5, 7.2, '低光照图像增强与目标检测实验流程图', 
            fontsize=14, ha='center', va='center', fontweight='bold')
    
    # 创建输出目录
    output_dir = 'figures'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"已创建目录: {output_dir}")
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'pipeline.png'), dpi=300, bbox_inches='tight')
    plt.savefig(os.path.join(output_dir, 'pipeline.pdf'), bbox_inches='tight')
    print(f"流程图已保存至 {output_dir}/pipeline.png 和 {output_dir}/pipeline.pdf")
    plt.show()

if __name__ == '__main__':
    draw_horizontal_flowchart()