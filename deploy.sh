#!/bin/bash

# ===================================
# 教育统计分析服务 - 快速部署脚本
# ===================================

set -e  # 遇到错误立即退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_message() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# 显示帮助信息
show_help() {
    echo "使用方法: ./deploy.sh [选项]"
    echo ""
    echo "选项:"
    echo "  --full        完整部署(包含所有服务)"
    echo "  --basic       基础部署(仅主服务)"
    echo "  --update      更新部署"
    echo "  --stop        停止所有服务"
    echo "  --restart     重启所有服务"
    echo "  --status      查看服务状态"
    echo "  --logs        查看服务日志"
    echo "  --clean       清理Docker资源"
    echo "  --help        显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  ./deploy.sh --basic    # 基础部署"
    echo "  ./deploy.sh --full     # 完整部署"
    echo "  ./deploy.sh --update   # 更新服务"
}

# 检查必要的软件
check_requirements() {
    print_message "检查系统要求..."
    
    # 检查Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker未安装！请先安装Docker"
        echo "安装命令: curl -fsSL https://get.docker.com | sh"
        exit 1
    fi
    
    # 检查Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose未安装！请先安装Docker Compose"
        echo "安装命令: sudo curl -L \"https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)\" -o /usr/local/bin/docker-compose"
        echo "         sudo chmod +x /usr/local/bin/docker-compose"
        exit 1
    fi
    
    # 显示版本信息
    print_message "Docker版本: $(docker --version)"
    print_message "Docker Compose版本: $(docker-compose --version)"
}

# 检查环境配置
check_environment() {
    print_message "检查环境配置..."
    
    # 检查.env文件
    if [ ! -f .env ]; then
        if [ -f .env.example ]; then
            print_warning ".env文件不存在，从模板创建..."
            cp .env.example .env
            print_error "请编辑.env文件，配置数据库连接等参数"
            echo "使用命令: vim .env"
            exit 1
        else
            print_error ".env和.env.example文件都不存在！"
            exit 1
        fi
    fi
    
    # 创建必要的目录
    print_message "创建必要的目录..."
    mkdir -p logs temp reports
    chmod -R 755 logs temp reports
}

# 基础部署
basic_deploy() {
    print_message "开始基础部署..."
    
    check_requirements
    check_environment
    
    print_message "构建Docker镜像..."
    docker-compose build app
    
    print_message "启动主服务..."
    docker-compose up -d app
    
    print_message "等待服务启动..."
    sleep 10
    
    # 健康检查
    if curl -f http://localhost:8010/health &> /dev/null; then
        print_message "部署成功！"
        echo ""
        echo "服务已启动:"
        echo "  - API文档: http://localhost:8010/docs"
        echo "  - 健康检查: http://localhost:8010/health"
        echo ""
        echo "查看日志: docker-compose logs -f app"
    else
        print_error "服务启动失败！"
        echo "查看日志获取详细信息:"
        docker-compose logs --tail=50 app
        exit 1
    fi
}

# 完整部署
full_deploy() {
    print_message "开始完整部署..."
    
    check_requirements
    check_environment
    
    print_message "构建所有镜像..."
    docker-compose build
    
    print_message "启动所有服务..."
    docker-compose up -d app subjects
    
    # 询问是否启用可选服务
    read -p "是否启用Redis缓存? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose --profile cache up -d
    fi
    
    read -p "是否启用批处理服务? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose --profile batch up -d
    fi
    
    print_message "等待服务启动..."
    sleep 15
    
    # 健康检查
    if curl -f http://localhost:8010/health &> /dev/null; then
        print_message "部署成功！"
        echo ""
        echo "服务已启动:"
        echo "  - 主API: http://localhost:8010/docs"
        echo "  - 科目API: http://localhost:8011/docs"
        echo ""
        docker-compose ps
    else
        print_error "服务启动失败！"
        docker-compose logs --tail=50
        exit 1
    fi
}

# 更新部署
update_deploy() {
    print_message "更新部署..."
    
    # 备份当前配置
    if [ -f .env ]; then
        cp .env .env.backup.$(date +%Y%m%d_%H%M%S)
        print_message "已备份当前配置"
    fi
    
    # 拉取最新代码(如果是Git仓库)
    if [ -d .git ]; then
        print_message "拉取最新代码..."
        git pull
    fi
    
    print_message "重新构建镜像..."
    docker-compose build --no-cache
    
    print_message "更新服务..."
    docker-compose up -d --no-deps app subjects
    
    print_message "清理旧镜像..."
    docker image prune -f
    
    print_message "更新完成！"
}

# 停止服务
stop_services() {
    print_message "停止所有服务..."
    docker-compose down
    print_message "服务已停止"
}

# 重启服务
restart_services() {
    print_message "重启所有服务..."
    docker-compose restart
    print_message "服务已重启"
}

# 查看状态
show_status() {
    print_message "服务状态:"
    docker-compose ps
    echo ""
    print_message "资源使用:"
    docker stats --no-stream
}

# 查看日志
show_logs() {
    print_message "显示最近100行日志..."
    docker-compose logs --tail=100 -f
}

# 清理Docker资源
clean_docker() {
    print_warning "这将清理所有未使用的Docker资源"
    read -p "确定要继续吗? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_message "清理Docker资源..."
        docker system prune -a -f
        print_message "清理完成"
    fi
}

# 主函数
main() {
    case "$1" in
        --basic)
            basic_deploy
            ;;
        --full)
            full_deploy
            ;;
        --update)
            update_deploy
            ;;
        --stop)
            stop_services
            ;;
        --restart)
            restart_services
            ;;
        --status)
            show_status
            ;;
        --logs)
            show_logs
            ;;
        --clean)
            clean_docker
            ;;
        --help)
            show_help
            ;;
        *)
            echo "教育统计分析服务 - 部署脚本"
            echo ""
            show_help
            ;;
    esac
}

# 执行主函数
main "$@"